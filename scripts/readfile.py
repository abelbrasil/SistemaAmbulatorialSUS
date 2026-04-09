from pathlib import Path
import pandas as pd

# ==============================
# PARAMETROS
# ==============================

FILE_NAME = "PASIA.parquet"

COLUNAS_FREQ = ["PA_CODUNI", "PA_MVM"]

# ==============================
# PATH BASE
# ==============================

BASE_DIR = Path(__file__).resolve().parents[1]
SILVER_DIR = BASE_DIR / "data" / "silver"

# ==============================
# FUNÇÃO
# ==============================

def read_file(file_name: str, colunas_freq: list):
    file_path = SILVER_DIR / file_name

    if not file_path.exists():
        print(f"\n❌ Arquivo não encontrado: {file_path}")
        return

    print(f"\n📄 Arquivo: {file_name}")

    df = pd.read_parquet(file_path)

    print(f"🔢 Linhas: {df.shape[0]}")
    print(f"📊 Colunas: {len(df.columns)}")

    print("\n🧾 Colunas disponíveis:")
    print(df.columns.tolist())

    print("\n🔍 Primeiras linhas:")
    print(df.head())

    # ==============================
    # FREQUÊNCIA
    # ==============================

    print("\n📊 TABELAS DE FREQUÊNCIA")

    for col in colunas_freq:
        if col not in df.columns:
            print(f"\n⚠️ Coluna não encontrada: {col}")
            continue

        print(f"\n🔹 Frequência da coluna: {col}")

        freq = (
            df[col]
            .astype(str)
            .value_counts(dropna=False)
            .reset_index()
        )

        freq.columns = [col, "FREQUENCIA"]

        # 🔥 ORDENAÇÃO PARA PA_MVM
        if col == "PA_MVM":
            freq = freq.sort_values(by=col)

        print(freq)


# ==============================
# EXECUÇÃO
# ==============================

if __name__ == "__main__":
    read_file(FILE_NAME, COLUNAS_FREQ)

# poetry run python scripts/readfile.py