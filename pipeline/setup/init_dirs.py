from pathlib import Path

# ==============================
# BASE SIMPLES
# ==============================

BASE_DIR = Path.cwd() 

DATA_DIR = BASE_DIR / "data"

# ==============================
# ESTRUTURA
# ==============================

DIRECTORIES = [
    DATA_DIR,

    DATA_DIR / "arq_aux",
    DATA_DIR / "arq_aux" / "tab_dim",
    DATA_DIR / "arq_aux" / "tmp_siadef",

    DATA_DIR / "bronze",
    DATA_DIR / "db",
    DATA_DIR / "gold",
    DATA_DIR / "silver",
    DATA_DIR / "silver_cnes",
]

# ==============================
# EXECUÇÃO
# ==============================

def create_directories():
    print("📁 Criando estrutura de diretórios...")

    for directory in DIRECTORIES:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"✔ {directory}")

    print("\n✅ Estrutura criada!")


if __name__ == "__main__":
    create_directories()

# poetry run python pipeline/setup/init_dirs.py