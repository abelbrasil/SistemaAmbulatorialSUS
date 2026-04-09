0 - ingest_ftp # baixa o arquivo .dbc
1 - transform_parquet # baixa atraves do pysus e guarda em parquet
poetry run python pipeline/transform_parquet.py
2 - silver_select # seleciona o cnes em cada arquivo parquet
poetry run python pipeline/silver_select.py
3 - silver_trat # combina os arquivos e seleciona as colunas de interesse
poetry run python pipeline/silver_trat.py
4 - gold_indicadores # cria os cubos para as tabelas fatos e dimensao
poetry run python pipeline/gold_indicadores.py

5 - database # estabelece a conexao com o banco SQLite
6 - models # configura a tipologia das tabelas fato e dimensao
7 - data_loader # carrega os dados no banco SQlite
8 - queries # define as consultas que deverao ser expostas
9 - router # rotas das API
10 - main # arquivo de executor
poetry run uvicorn backend.main:app --reload

# FrontEnd

11 - app # codigos do painel 
poetry run streamlit run frontend/app.py 

# Scritps Auxiliares

11 - create_db # codigo para criar um banco SQlite
12 - siadef # codigo para transferir arquivos de definicao do tabwin
13 - tabdim # exporta o arquivo de definicao do SIA em parquet
14 - readfile # codigo para visualizacao dos dados
15 - readshowtab # codigo para visualizacao dos dados
16 - testquery # codigo para visualizar as consultas do arquivo queries

FTP DATASUS
     ↓
[BRONZE]  → .dbc
     ↓
[SILVER]  → Parquet
     ↓
[GOLD]    → SQLite
     ↓
FastAPI
     ↓
Streamlit


datasus-streamlit-app/
│
├── backend/                      # FastAPI
│   ├── app/
│   │   ├── main.py
│   │   │
│   │   ├── api/
│   │   │   └── routes/
│   │   │       ├── producao.py
│   │   │       └── hospitais.py
│   │   │
│   │   ├── services/
│   │   │   └── producao_service.py
│   │   │
│   │   ├── repositories/
│   │   │   └── producao_repository.py
│   │   │
│   │   ├── models/
│   │   │   └── schema.py
│   │   │
│   │   └── core/
│   │       └── database.py
│   │
│   └── requirements.txt
│
├── app/                          # Streamlit
│   ├── app.py
│   ├── pages/
│   └── services/
│       └── api_client.py
│
├── pipeline/                     # ETL Medalhão
│   │
│   ├── bronze/
│   │   └── ingest_ftp.py
│   │
│   ├── silver/
│   │   └── transform_parquet.py
│   │
│   ├── gold/
│   │   └── load_to_db.py
│   │
│   └── run_pipeline.py
│
├── data/
│   │
│   ├── bronze/
│   │   └── raw_dbc/
│   │
│   ├── silver/
│   │   └── parquet/
│   │       └── producao.parquet
│   │
│   └── gold/                     # Banco local
│       └── db.sqlite3
│
├── utils/
│   └── config.py
│
├── requirements.txt
└── README.md