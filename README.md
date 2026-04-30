# 📊 Projeto SIASUS -- Pipeline de Dados + API + Dashboard

Este projeto implementa uma arquitetura completa de dados baseada no
**DATASUS (SIASUS)**, contemplando:

-   🔄 Pipeline de dados (ETL)
-   🗄️ Camadas Bronze / Silver / Gold
-   🚀 API REST com FastAPI
-   📊 Dashboard interativo com Streamlit
-   🐳 Infraestrutura containerizada com Docker

------------------------------------------------------------------------

# 🧱 Arquitetura do Projeto

Pipeline (ETL) → Dados (Parquet / SQLite) → Backend (API) → Frontend
(Dashboard)

------------------------------------------------------------------------

# 📁 Estrutura do Projeto

    .
    ├── backend/
    ├── frontend/
    ├── pipeline/
    ├── data/
    ├── docs/
    ├── tests/
    ├── docker-compose.yml
    ├── pyproject.toml
    └── rodar_pipeline.py

------------------------------------------------------------------------

# 🚀 Reset do ambiente

    docker-compose down --volumes --remove-orphans
    docker image prune -a -f
    docker volume prune -f
    rm -rf data

# 🚀 Execução Completa

    git clone https://github.com/abelbrasil/SistemaAmbulatorialSUS.git
    cd SistemaAmbulatorialSUS

    cp .env.example .env 

    docker-compose build --no-cache
    docker-compose up -d

    sleep 5

    docker-compose run --rm pipeline

------------------------------------------------------------------------

# 🐳 Validação

    docker ps
    docker logs backend_api
    docker-compose exec backend ls /app/data

------------------------------------------------------------------------

# 🌐 Acesso

-   API: http://localhost:8000/docs
-   Dashboard: http://localhost:8501

------------------------------------------------------------------------

# 🧪 Testes

    make test

------------------------------------------------------------------------

# ⚠️ Boas Práticas

-   Rodar pipeline após subir containers
-   Pipeline é batch
-   Garantir diretórios de dados

------------------------------------------------------------------------

# 👨‍💻 Autor

Abel Brasil Ramos da Silva