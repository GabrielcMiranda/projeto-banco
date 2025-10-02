import pandas as pd
import psycopg2
from datetime import datetime
import json
from os import getenv
from dotenv import load_dotenv

# Configurações de conexão com o PostgreSQL
load_dotenv()
DB_CONFIG = {
    'host': getenv('DB_HOST'),
    'dbname': getenv('DB_NAME'),
    'user': getenv('DB_USER'),
    'password': getenv('DB_PASSWORD'),
    'port': 5432
}

def conectar_db():
    """Estabelece conexão com o banco de dados"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        raise

def limpar_valor_numerico(valor):
    """Remove caracteres não numéricos e converte para número"""
    if pd.isna(valor) or valor == '':
        return None
    if isinstance(valor, str):
        valor = valor.replace(',', '').replace('$', '').strip()
    try:
        return float(valor)
    except:
        return None

def processar_lista(valor, separador=','):
    """Processa campos que contêm listas separadas por vírgula ou JSON"""
    if pd.isna(valor) or valor == '':
        return []

    # Tenta parsear como JSON primeiro
    if isinstance(valor, str):
        v = valor.strip()
        # Remove aspas simples/duplas externas
        if (v.startswith("[") and v.endswith("]")) or (v.startswith("'") and v.endswith("']")):
            v = v.strip("[]'")
        # Remove tags HTML
        v = v.replace('&amp;lt;strong&amp;gt;', '').replace('&amp;lt;/strong&amp;gt;', '')
        v = v.replace('<strong>', '').replace('</strong>', '')
        # Tenta JSON
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list):
                return [str(item).strip("'\" ") for item in parsed if item]
        except:
            pass
        # Separa por vírgula, fecha colchete, ou aspas
        items = []
        for sep in [",", "]", "'", "\n"]:
            if sep in v:
                items = [i.strip("'\" []") for i in v.split(sep) if i.strip("'\" []")]
                if len(items) > 1:
                    break
        if not items:
            items = [v.strip("'\" []")]
        return [i for i in items if i]
    # Se for lista
    if isinstance(valor, list):
        return [str(item).strip("'\" ") for item in valor if item]
    return []

def inserir_tabelas_referencia(conn, df):
    """Insere dados nas tabelas de referência e retorna dicionários de mapeamento"""
    cursor = conn.cursor()
    
    # Plataformas (fixas)
    platforms = {'Windows': None, 'Mac': None, 'Linux': None}
    for platform_name in platforms.keys():
        cursor.execute(
            "INSERT INTO Platforms (Name) VALUES (%s) ON CONFLICT (Name) DO UPDATE SET Name=EXCLUDED.Name RETURNING PlatformId",
            (platform_name,)
        )
        platforms[platform_name] = cursor.fetchone()[0]
    
    # Developers
    developers = {}
    all_developers = set()
    for devs in df['Developers'].dropna():
        all_developers.update(processar_lista(devs))
    
    for dev in all_developers:
        if dev:
            cursor.execute(
                "INSERT INTO Developers (Name) VALUES (%s) ON CONFLICT (Name) DO UPDATE SET Name=EXCLUDED.Name RETURNING DeveloperID",
                (dev,)
            )
            developers[dev] = cursor.fetchone()[0]
    
    # Publishers
    publishers = {}
    all_publishers = set()
    for pubs in df['Publishers'].dropna():
        all_publishers.update(processar_lista(pubs))
    
    for pub in all_publishers:
        if pub:
            cursor.execute(
                "INSERT INTO Publishers (Name) VALUES (%s) ON CONFLICT (Name) DO UPDATE SET Name=EXCLUDED.Name RETURNING PublisherID",
                (pub,)
            )
            publishers[pub] = cursor.fetchone()[0]
    
    # Categories
    categories = {}
    all_categories = set()
    for cats in df['Categories'].dropna():
        all_categories.update(processar_lista(cats))
    
    for cat in all_categories:
        if cat:
            cursor.execute(
                "INSERT INTO Categories (Name) VALUES (%s) ON CONFLICT (Name) DO UPDATE SET Name=EXCLUDED.Name RETURNING CategorieID",
                (cat,)
            )
            categories[cat] = cursor.fetchone()[0]
    
    # Genres
    genres = {}
    all_genres = set()
    for gens in df['Genres'].dropna():
        all_genres.update(processar_lista(gens))
    
    for gen in all_genres:
        if gen:
            cursor.execute(
                "INSERT INTO Genres (Name) VALUES (%s) ON CONFLICT (Name) DO UPDATE SET Name=EXCLUDED.Name RETURNING GenreID",
                (gen,)
            )
            genres[gen] = cursor.fetchone()[0]
    
    # Tags
    tags = {}
    all_tags = set()
    for tag_list in df['Tags'].dropna():
        all_tags.update(processar_lista(tag_list))
    
    for tag in all_tags:
        if tag:
            cursor.execute(
                "INSERT INTO Tags (Name) VALUES (%s) ON CONFLICT (Name) DO UPDATE SET Name=EXCLUDED.Name RETURNING TagID",
                (tag,)
            )
            tags[tag] = cursor.fetchone()[0]
    
    # Languages
    languages = {}
    all_languages = set()
    for langs in df['Supported languages'].dropna():
        all_languages.update(processar_lista(langs))
    for langs in df['Full audio languages'].dropna():
        all_languages.update(processar_lista(langs))
    
    for lang in all_languages:
        if lang:
            cursor.execute(
                "INSERT INTO Languages (Name) VALUES (%s) ON CONFLICT (Name) DO UPDATE SET Name=EXCLUDED.Name RETURNING LanguageID",
                (lang,)
            )
            languages[lang] = cursor.fetchone()[0]
    
    conn.commit()
    cursor.close()
    
    return {
        'platforms': platforms,
        'developers': developers,
        'publishers': publishers,
        'categories': categories,
        'genres': genres,
        'tags': tags,
        'languages': languages
    }

def processar_data(data_str):
    """Converte string de data para formato DATE"""
    if pd.isna(data_str) or data_str == '':
        return None
    formatos = ['%b %d, %Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%b %Y']
    for formato in formatos:
        try:
            return datetime.strptime(str(data_str).strip(), formato).date()
        except:
            continue
    print(f"Data não convertida: {data_str}")
    return None

def migrar_dados(arquivo_csv):
    """Função principal de migração"""
    print("Carregando arquivo CSV...")
    df = pd.read_csv(arquivo_csv, low_memory=False, index_col=False,encoding='utf-8')
    
    print(f"Total de registros: {len(df)}")
    
    conn = conectar_db()
    cursor = conn.cursor()
    mapeamentos = inserir_tabelas_referencia(conn, df)

    for idx, row in df.iterrows():
        if idx % 100 == 0:
            print(f"Processando registro {idx + 1}/{len(df)}...")

        app_id = int(row['AppID']) if pd.notna(row['AppID']) else None
        if not app_id:
            continue

        try:
            cursor.execute("""
                INSERT INTO Games (AppID, Name, Release_date, Required_age, About_the_game, Header_image, Notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (AppID) DO NOTHING
            """, (
                app_id,
                row['Name'] if pd.notna(row['Name']) else None,
                processar_data(row['Release date']),
                int(row['Required age']) if pd.notna(row['Required age']) else None,
                row['About the game'] if pd.notna(row['About the game']) else None,
                row['Header image'] if pd.notna(row['Header image']) else None,
                row['Notes'] if pd.notna(row['Notes']) else None
            ))
            conn.commit()
        except psycopg2.errors.UniqueViolation:
            print(f"Registro duplicado: AppID={app_id}, Name={row['Name']}")
            conn.rollback()
            continue
        except Exception as e:
            print(f"Erro inesperado ao inserir jogo {app_id}: {e}")
            conn.rollback()
            continue

        # Verifica se o AppID existe na tabela games antes de inserir dados relacionados
        # Relacionamentos muitos-para-muitos
        # Game_Genres
        for genre_name in processar_lista(row.get('Genres', '')):
            genre_id = mapeamentos['genres'].get(genre_name)
            if genre_id:
                cursor.execute("""
                    INSERT INTO Game_Genres (AppID, GenreID)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (app_id, genre_id))
        # Game_Tags
        for tag_name in processar_lista(row.get('Tags', '')):
            tag_id = mapeamentos['tags'].get(tag_name)
            if tag_id:
                cursor.execute("""
                    INSERT INTO Game_Tags (AppID, TagID)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (app_id, tag_id))
        # Game_Categories
        for cat_name in processar_lista(row.get('Categories', '')):
            cat_id = mapeamentos['categories'].get(cat_name)
            if cat_id:
                cursor.execute("""
                    INSERT INTO Game_Categories (AppID, CategorieID)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (app_id, cat_id))
        # Game_Developers
        for dev_name in processar_lista(row.get('Developers', '')):
            dev_id = mapeamentos['developers'].get(dev_name)
            if dev_id:
                cursor.execute("""
                    INSERT INTO Game_Developers (AppID, DeveloperID)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (app_id, dev_id))
        # Game_Publishers
        for pub_name in processar_lista(row.get('Publishers', '')):
            pub_id = mapeamentos['publishers'].get(pub_name)
            if pub_id:
                cursor.execute("""
                    INSERT INTO Game_Publishers (AppID, PublisherID)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (app_id, pub_id))
        # PlatformGames
        for platform_name in mapeamentos['platforms']:
            if pd.notna(row.get(platform_name)) and row.get(platform_name):
                platform_id = mapeamentos['platforms'][platform_name]
                cursor.execute("""
                    INSERT INTO GamePlatforms (AppID, PlatformId)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (app_id, platform_id))
        cursor.execute("SELECT 1 FROM Games WHERE AppID = %s", (app_id,))
        existe_game = cursor.fetchone() is not None
        if not existe_game:
            continue

        try:
            # GameContacts
            cursor.execute("""
                INSERT INTO GameContacts (AppID, Website, Support_url, Support_email)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (AppID) DO NOTHING
            """, (
                app_id,
                row['Website'] if pd.notna(row['Website']) else None,
                row['Support url'] if pd.notna(row['Support url']) else None,
                row['Support email'] if pd.notna(row['Support email']) else None
            ))
            # GamePricing
            price = limpar_valor_numerico(row['Price'])
            discount = limpar_valor_numerico(row['Discount'])
            if price is not None:
                cursor.execute("""
                    INSERT INTO GamePricing (AppID, Price, Discount)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (AppID) DO NOTHING
                """, (app_id, price, discount))
            # GameReviews
            cursor.execute("""
                INSERT INTO GameReviews (AppID, Positive, Negative, User_score, Score_rank, Recommendations)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (AppID) DO NOTHING
            """, (
                app_id,
                int(row['Positive']) if pd.notna(row['Positive']) else None,
                int(row['Negative']) if pd.notna(row['Negative']) else None,
                limpar_valor_numerico(row['User score']),
                int(row['Score rank']) if pd.notna(row['Score rank']) else None,
                int(row['Recommendations']) if pd.notna(row['Recommendations']) else None
            ))
            # Reviews (texto das avaliações)
            if pd.notna(row['Reviews']):
                # Split robusto: quebra por aspas, ou por quebras de linha, ou por pontuação forte
                raw_reviews = str(row['Reviews']).strip()
                # Tenta separar por aspas, se houver
                if '“' in raw_reviews and '”' in raw_reviews:
                    reviews_list = [r.strip() for r in raw_reviews.split('”') if r.strip()]
                    reviews_list = [r.split('“')[-1].strip() for r in reviews_list if '“' in r]
                else:
                    # Se não houver aspas, separa por quebra de linha ou ponto final seguido de aspas
                    reviews_list = [r.strip() for r in raw_reviews.split('\n') if r.strip()]
                    if len(reviews_list) == 1:
                        # Se não quebrou, tenta por ponto final seguido de aspas ou por ponto final
                        reviews_list = [r.strip() for r in raw_reviews.split('.”') if r.strip()]
                        if len(reviews_list) == 1:
                            reviews_list = [r.strip() for r in raw_reviews.split('.') if r.strip()]
                for review in reviews_list:
                    if review:
                        cursor.execute("""
                            INSERT INTO Reviews (AppID, ReviewText)
                            VALUES (%s, %s)
                        """, (app_id, review))
            # GameStats
            cursor.execute("""
                INSERT INTO GameStats (AppID, Estimated_owners, Peak_CCU, DLC_Count, Achievements,
                                      Average_playtime_forever, Average_playtime_two_weeks,
                                      Median_playtime_forever, Median_playtime_two_weeks)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (AppID) DO NOTHING
            """, (
                app_id,
                row['Estimated owners'] if pd.notna(row['Estimated owners']) else None,
                int(row['Peak CCU']) if pd.notna(row['Peak CCU']) else None,
                int(row['DLC count']) if pd.notna(row['DLC count']) else None,
                int(row['Achievements']) if pd.notna(row['Achievements']) else None,
                int(row['Average playtime forever']) if pd.notna(row['Average playtime forever']) else None,
                int(row['Average playtime two weeks']) if pd.notna(row['Average playtime two weeks']) else None,
                int(row['Median playtime forever']) if pd.notna(row['Median playtime forever']) else None,
                int(row['Median playtime two weeks']) if pd.notna(row['Median playtime two weeks']) else None
            ))
            # Game_Languages
            supported_langs = set(processar_lista(row['Supported languages']))
            full_audio_langs = set(processar_lista(row['Full audio languages']))
            for lang_name in supported_langs:
                if lang_name in mapeamentos['languages']:
                    has_audio = lang_name in full_audio_langs
                    cursor.execute("""
                        INSERT INTO Game_Languages (AppID, LanguageID, Full_audio)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (app_id, mapeamentos['languages'][lang_name], has_audio))
            # Metacritic
            if pd.notna(row['Metacritic score']) or pd.notna(row['Metacritic url']):
                cursor.execute("""
                    INSERT INTO Metacritic (AppID, MetacriticScore, MetacriticUrl)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (AppID) DO NOTHING
                """, (
                    app_id,
                    int(row['Metacritic score']) if pd.notna(row['Metacritic score']) else None,
                    row['Metacritic url'] if pd.notna(row['Metacritic url']) else None
                ))
            # Screenshots
            for screenshot_url in processar_lista(row['Screenshots']):
                if screenshot_url:
                    cursor.execute("""
                        INSERT INTO Screenshots (AppID, Url)
                        VALUES (%s, %s)
                        ON CONFLICT (Url) DO NOTHING
                    """, (app_id, screenshot_url))
            # Movies
            for movie_url in processar_lista(row['Movies']):
                if movie_url:
                    cursor.execute("""
                        INSERT INTO Movies (AppID, Url)
                        VALUES (%s, %s)
                        ON CONFLICT (Url) DO NOTHING
                    """, (app_id, movie_url))
        except Exception as e:
            print(f"Erro ao inserir dados do jogo {app_id}: {e}")
            conn.rollback()
            continue

if __name__ == "__main__":
    
    arquivo_csv = 'games.csv'
    
    print("Iniciando migração de dados...")
    migrar_dados(arquivo_csv)