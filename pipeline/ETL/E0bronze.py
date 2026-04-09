from pathlib import Path
from datetime import datetime
from pysus import SIA

# ==============================
# CONFIGURAÇÃO BASE
# ==============================

BASE_DIR = Path.cwd()
BRONZE_DIR = BASE_DIR / "data" / "bronze"

GROUP = "PA"  # Produção Ambulatorial

# ==============================
# LOGGER
# ==============================

def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ==============================
# PIPELINE BRONZE
# ==============================

def run_bronze(uf: str, year: int, months: list[int]):
    log("=== INÍCIO CAMADA BRONZE (pysus) ===")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # Inicializa SIA
    sia = SIA().load()

    log("Conectado ao SIA/DATASUS")

    # Busca arquivos
    log(f"Buscando arquivos: UF={uf}, Ano={year}, Meses={months}")

    files = sia.get_files(
        GROUP,
        uf=uf,
        year=year,
        month=months
    )

    if not files:
        raise Exception("Nenhum arquivo encontrado")

    log(f"{len(files)} arquivos encontrados")

    # Download
    log("Iniciando download...")

    downloaded_files = sia.download(
        files,
        local_dir=str(BRONZE_DIR)
    )

    log(f"{len(downloaded_files)} arquivos baixados")

    for f in downloaded_files:
        log(f"[OK] {f}")

    log("=== FIM CAMADA BRONZE ===")


# ==============================
# EXECUÇÃO LOCAL (OPCIONAL)
# ==============================

if __name__ == "__main__":
    run_bronze(
        uf="CE",
        year=2024,
        months=[1,2,3]
    )