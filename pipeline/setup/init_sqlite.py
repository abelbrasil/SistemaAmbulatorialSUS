from pathlib import Path
from datetime import datetime

# ==============================
# CONFIG
# ==============================

BASE_DIR = Path.cwd()

DB_DIR = BASE_DIR / "data" / "db"
DB_PATH = DB_DIR / "indicadores.db"

# ==============================
# LOGGER
# ==============================

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# ==============================
# CRIAÇÃO DO BANCO
# ==============================

def create_database():
    log("=== INÍCIO CRIAÇÃO DATABASE ===")

    DB_DIR.mkdir(parents=True, exist_ok=True)

    if DB_PATH.exists():
        log(f"[INFO] Banco já existe: {DB_PATH}")
    else:
        DB_PATH.touch()
        log(f"[OK] Banco criado: {DB_PATH}")

    log("=== FIM CRIAÇÃO DATABASE ===")


# ==============================
# EXECUÇÃO
# ==============================

if __name__ == "__main__":
    create_database()