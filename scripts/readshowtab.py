from pathlib import Path
import pandas as pd

# ==============================
# PARAMETRO (ALTERE AQUI)
# ==============================

FILE_NAME = "dim_cnes.parquet"

# ==============================
# CONFIGURAÇÃO
# ==============================

BASE_DIR = Path.cwd() 

#TAB_DIM_DIR = BASE_DIR / "data" / "arq_aux" / "tab_dim"
TAB_DIM_DIR = BASE_DIR / "data" / "gold"
FILE_PATH = TAB_DIM_DIR / FILE_NAME

# ==============================
# FUNÇÃO
# ==============================

def run():
    print("\n=== LEITURA TAB_DIM ===")

    if not FILE_PATH.exists():
        print(f"\n❌ Arquivo não encontrado: {FILE_PATH}")
        return

    print(f"\n📄 Arquivo: {FILE_NAME}")

    df = pd.read_parquet(FILE_PATH)

    print(f"\n🔢 Linhas: {df.shape[0]}")
    print(f"📊 Colunas: {df.shape[1]}")

    print("\n🧾 Colunas:")
    print(df.columns.tolist())

    print("\n🔍 Primeiras linhas:")
    print(df.head())
    #print(df)


# ==============================
# EXECUÇÃO
# ==============================

if __name__ == "__main__":
    run()

# poetry run python scripts/readshowtab.py