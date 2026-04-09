# 01StreamlitV1 - Sistema de Análise de Dados DATASUS

Um sistema completo de ETL + API + Dashboard para análise de dados de produção hospitalar do **DATASUS** (Ministério da Saúde do Brasil).

## 📋 Visão Geral

Este projeto consome dados de saúde pública disponibilizados pelo DATASUS, processa através de um pipeline ETL seguindo o padrão Medalhão (Bronze → Silver → Gold), e expõe uma API REST com um dashboard interativo para visualização e análise.

### Arquitetura

```
FTP DATASUS (.dbc)
      ↓
[BRONZE] - Raw data
      ↓
[SILVER] - Transformed Parquet
      ↓
[GOLD] - SQLite Database
      ↓
FastAPI (Backend)
      ↓
Streamlit (Frontend)
```

## 🚀 Quick Start

### Pré-requisitos

- **Python 3.12+**
- **Poetry** (gerenciador de dependências)
- **Git**

### 1. Clonar e Configurar Ambiente

```bash
# Clonar o repositório
git clone <seu-repositorio>
cd 01StreamlitV1

# Definir versão Python (opcional, se usar pyenv)
pyenv local 3.12

# Instalar dependências
poetry install

# Ativar ambiente virtual
source .venv/bin/activate
# ou no Windows: .venv\Scripts\activate
```

### 2. Executar o Pipeline (ETL)

```bash
# Executar todas as etapas do pipeline
poetry run python pipeline/ingest_ftp.py           # Bronze: baixar .dbc do FTP
poetry run python pipeline/transform_parquet.py    # Silver: converter para Parquet
poetry run python pipeline/silver_select.py        # Silver: selecionar CNES
poetry run python pipeline/silver_trat.py          # Silver: tratar e combinar
poetry run python pipeline/gold_indicadores.py     # Gold: criar cubos e carregar BD

# Ou criar scripts para executar sequencialmente
```

### 3. Iniciar o Backend (API)

```bash
poetry run uvicorn backend.main:app --reload
```

A API estará disponível em: `http://localhost:8000`
Documentação interativa: `http://localhost:8000/docs`

### 4. Iniciar o Frontend (Dashboard)

Em outro terminal:

```bash
poetry run streamlit run frontend/app.py
```

O dashboard abrirá em: `http://localhost:8501`

## 📁 Estrutura do Projeto

```
01StreamlitV1/
│
├── backend/                        # FastAPI Backend
│   ├── main.py                    # Aplicação principal
│   ├── database.py                # Configuração do banco de dados
│   ├── models.py                  # Esquemas e modelos
│   ├── queries.py                 # Queries SQL pré-definidas
│   ├── router.py                  # Rotas API
│   └── data_loader.py             # Carregamento de dados no BD
│
├── frontend/                       # Streamlit Frontend
│   └── app.py                     # Aplicação Streamlit
│
├── pipeline/                       # ETL Pipeline (Medalhão)
│   ├── ingest_ftp.py              # Bronze: Ingestão FTP
│   ├── transform_parquet.py       # Silver: Transformação para Parquet
│   ├── silver_select.py           # Silver: Seleção de CNES
│   ├── silver_trat.py             # Silver: Tratamento de dados
│   └── gold_indicadores.py        # Gold: Cubos e Indicadores
│
├── data/                          # Diretórios de dados
│   ├── bronze/                    # Raw .dbc files
│   ├── silver/                    # Parquet transformados
│   └── gold/                      # Banco SQLite
│
├── scripts/                       # Utilitários auxiliares
│   ├── create_db.py              # Criar banco de dados
│   ├── siadef.py                 # Transferir definições
│   ├── tabdim.py                 # Exportar definições
│   ├── readfile.py               # Visualizar dados
│   └── testquery.py              # Testar queries
│
├── pyproject.toml                 # Dependências do projeto (Poetry)
└── README.md                      # Este arquivo
```

## 🔄 Pipeline de Dados (ETL)

### Bronze Layer
- **Fonte**: FTP do DATASUS
- **Formato**: Arquivos `.dbc` compactados
- **Descrição**: Dados brutos, sem tratamento

### Silver Layer
- **Formato**: Parquet particionado
- **Transformações**:
  - Conversão de `.dbc` para Parquet
  - Seleção por CNES (Cadastro Nacional de Estabelecimentos de Saúde)
  - Limpeza e validação de dados
  - Combinação de múltiplos arquivos
- **Qualidade**: Dados validados e padronizados

### Gold Layer
- **Formato**: Banco de dados SQLite
- **Estrutura**: Schema com tabelas fato e dimensão
- **Indicadores**: Cubos e agregações para análise

## 📊 Endpoints da API

### Documentação Automática
- Swagger UI: `GET /docs`
- ReDoc: `GET/redoc`

### Principais Endpoints
```
GET  /api/producao              # Listar dados de produção
GET  /api/producao/{id}         # Obter produção específica
GET  /api/hospitais             # Listar hospitais (CNES)
GET  /api/hospitais/{cnes}      # Obter dados específicos do hospital
```

Acesse `http://localhost:8000/docs` para explorar interativamente.

## 🎨 Dashboard Streamlit

O dashboard oferece:
- Visualização interativa de dados
- Filtros por período, CNES, indicadores
- Gráficos e tabelas dinâmicas
- Exportação de dados

## 🛠️ Desenvolvimento

### Adicionar Novas Dependências

```bash
poetry add nome-da-dependencia
# ou com versão específica
poetry add "nome-da-dependencia>=1.0,<2.0"
```

### Atualizar Dependências

```bash
poetry update
```

### Executar Testes

```bash
# Se houver testes no projeto
poetry run pytest tests/
```

## 🔧 Configuração

### Variáveis de Ambiente

Criar arquivo `.env` na raiz do projeto (se necessário):

```env
# Exemplo
DATABASE_URL=sqlite:///./data/gold/db.sqlite3
FTP_HOST=ftp.datasus.gov.br
API_HOST=localhost
API_PORT=8000
```

### Arquivos de Dados

- **Bronze**: `./data/bronze/` - Arquivos `.dbc` do FTP
- **Silver**: `./data/silver/` - Arquivos Parquet transformados
- **Gold**: `./data/gold/db.sqlite3` - Banco de dados SQLite

## ⚙️ Comandos Úteis

```bash
# Resetar banco de dados
poetry run python backend/resetdb.py

# Visualizar arquivo Parquet
poetry run python scripts/readfile.py

# Testar queries
poetry run python scripts/testquery.py

# Criar banco da zero
poetry run python scripts/create_db.py

# Ver estrutura SQLite
poetry run python scripts/readfile.py
```

## 🐛 Troubleshooting

### Erro ao conectar no FTP
- Verificar conexão de internet
- Verificar se o endereço FTP está correto em `pipeline/ingest_ftp.py`
- Arquivos `.dbc` podem estar temporariamente indisponíveis

### Banco de dados corrompido
```bash
# Resetar banco
poetry run python backend/resetdb.py

# Reexecutar pipeline
poetry run python pipeline/gold_indicadores.py
```

### Porta já em uso
```bash
# Backend em outra porta
poetry run uvicorn backend.main:app --reload --port 8001

# Streamlit em outra porta
poetry run streamlit run frontend/app.py --server.port 8502
```

### Problema com dependências
```bash
# Limpar cache de Poetry
poetry cache clear . --all

# Reinstalar
poetry install --no-cache
```

## 📚 Tecnologias Utilizadas

| Camada | Tecnologia | Versão |
|--------|-----------|--------|
| Linguagem | Python | 3.12+ |
| Gerenciador | Poetry | - |
| ETL | Pandas, PySUS | 2.2+, 1.0+ |
| Backend | FastAPI | 0.135+ |
| Frontend | Streamlit | 1.55+ |
| Database | SQLite + SQLAlchemy | - |
| Visualização | Plotly, Matplotlib | 6.6+, 3.10+ |

## 📖 Referências

- [DATASUS](https://datasus.saude.gov.br/)
- [PySUS - Acesso a dados do DATASUS](https://github.com/AlertasSUS/pysus)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [SQLAlchemy Documentation](https://www.sqlalchemy.org/)

## 👤 Autor

**Abel Brasil**
- Email: abelbrasil88@gmail.com

## 📝 Licença

Especifique a licença do projeto aqui.

## 🤝 Contribuições

Contribuições são bem-vindas! Por favor:
1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

---

**Última atualização**: Março 2024
