# Projeto Banco de Dados Steam

Este projeto realiza a migração de dados de um arquivo CSV para um banco de dados PostgreSQL, estruturando e normalizando informações de jogos da Steam.

## Pré-requisitos
- Python 3.8+
- PostgreSQL

## Instalação dos pacotes Python
Abra o terminal na pasta do projeto e execute:
```bash
pip install pandas psycopg2-binary python-dotenv
```

## Configuração do ambiente
1. Renomeie o arquivo `.env.example` para `.env`.
2. Preencha suas variáveis de ambiente conforme o exemplo:
	```env
	DB_NAME=seu_nome_do_banco
	DB_USER=seu_usuario
	DB_PASSWORD=sua_senha
	DB_HOST=localhost
	```

## Criação do banco e tabelas
1. Crie um banco de dados PostgreSQL com o nome definido em `DB_NAME`.
2. Abra o pgAdmin (ou outra ferramenta gráfica de administração do Postgres).
3. Acesse o banco criado, abra a Query Tool e copie/cole todo o conteúdo do arquivo `creates_projeto.txt` na janela de query.
4. Execute a query para criar todas as tabelas.
	*(O arquivo `creates_projeto.txt` contém todos os comandos de criação das tabelas.)*

## Migração dos dados
Execute o script de migração para importar os dados do CSV para o banco:
```bash
python migration.py
```

## Consultas e visualização
Após a migração, abra o pgAdmin (ou outra ferramenta gráfica de administração do Postgres), acesse o banco criado, abra a Query Tool e copie/cole todo o conteúdo do arquivo `selects_projeto.txt` na janela de query.
Execute a query para visualizar e analisar os dados.

## Estrutura dos arquivos
- `creates_projeto.txt`: Comandos SQL para criar as tabelas.
- `migration.py`: Script Python para migrar os dados do CSV para o banco.
- `selects_projeto.txt`: Consultas SQL para visualizar e analisar os dados.
- `.env.example`: Exemplo de configuração das variáveis de ambiente.
- `games.csv`: Arquivo de dados fonte.

## Observações
- Certifique-se de que o PostgreSQL está rodando e acessível.
- O arquivo `.env` não deve ser versionado para manter suas credenciais seguras.
- Caso encontre erros de dependências, revise o comando de instalação dos pacotes.

---
