from datetime import datetime

# ==============================
# IMPORTS - SETUP
# ==============================

from pipeline.setup.init_dirs import create_directories
from pipeline.setup.siadef import run_siadef
from pipeline.setup.tabdim import run_tabdim

# ==============================
# IMPORTS - ETL
# ==============================

from pipeline.ETL.E0bronze import run_bronze
from pipeline.ETL.E1silver_cnes import run_silver_cnes
from pipeline.ETL.E2silver_trat import run_silver_trat
from pipeline.ETL.E3gold import run_gold

# ==============================
# IMPORTS - LOAD
# ==============================

from pipeline.setup.init_sqlite import create_database
from pipeline.setup.init_schema import create_schema
from pipeline.load.data_loader import run_loader


# ==============================
# LOGGER
# ==============================

def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ==============================
# CONFIGURAÇÃO DO PIPELINE
# ==============================

UF = "CE"
YEAR = 2024
MONTHS = [1,2,3,4,5,6,7,8,9,10,11,12]

COD_UNIDADES = ["2561492", "2481286"]

RODAR_TUDO = False
PROCESSAR_ANO = None

# 🔥 CONTROLE DE EXECUÇÃO (EVOLUÇÃO)
EXECUTAR_SETUP = False # Setup é mais demorado, rodar apenas quando necessário (ex: nova dimensão, nova pasta, etc)
EXECUTAR_BRONZE = False # Bronze é o mais demorado, rodar apenas quando necessário (ex: nova UF, novo ano, etc)
EXECUTAR_SILVER = True # Silver é mais rápido, rodar sempre que rodar o pipeline (ex: nova regra de negócio, nova transformação, etc) 
EXECUTAR_GOLD = True # Gold é o mais rápido, rodar sempre que rodar o pipeline (ex: nova regra de negócio, nova transformação, etc) 
EXECUTAR_LOAD = True # Load é o mais rápido, rodar sempre que rodar o pipeline (ex: nova regra de negócio, nova transformação, etc)

# ==============================
# ORQUESTRADOR
# ==============================

def run_pipeline():
    log("🚀 INICIANDO PIPELINE COMPLETO")

    try:

        # ==============================
        # SETUP
        # ==============================
        if EXECUTAR_SETUP:
            log("📁 Etapa 1: Criando diretórios")
            create_directories()

            log("🌐 Etapa 2: Download arquivos auxiliares")
            run_siadef()

            log("📦 Etapa 3: Processando dimensões (DBF → Parquet)")
            run_tabdim()

        # ==============================
        # BRONZE
        # ==============================
        if EXECUTAR_BRONZE:
            log("🥉 Etapa 4: Bronze (download SIA)")
            run_bronze(
                uf=UF,
                year=YEAR,
                months=MONTHS
            )

        # ==============================
        # SILVER
        # ==============================
        if EXECUTAR_SILVER:
            log("🥈 Etapa 5: Silver CNES (filtro)")
            run_silver_cnes(
                cod_unidades=COD_UNIDADES,
                rodar_tudo=RODAR_TUDO,
                processar_ano=PROCESSAR_ANO
            )

            log("🥈 Etapa 6: Silver Tratamento")
            run_silver_trat(
                rodar_tudo=RODAR_TUDO,
                processar_ano=PROCESSAR_ANO
            )

        # ==============================
        # GOLD
        # ==============================
        if EXECUTAR_GOLD:
            log("🥇 Etapa 7: Gold (agregações)")
            run_gold()

        # ==============================
        # LOAD (DATABASE)
        # ==============================
        if EXECUTAR_LOAD:
            log("🗄️ Etapa 8: Criando banco")
            create_database()

            log("🏗️ Etapa 9: Criando schema")
            create_schema()

            log("📥 Etapa 10: Carregando dados no banco")
            run_loader()

        log("✅ PIPELINE FINALIZADO COM SUCESSO")

    except Exception as e:
        log(f"❌ ERRO NO PIPELINE: {e}")
        raise


# ==============================
# EXECUÇÃO
# ==============================

if __name__ == "__main__":
    run_pipeline()