from pathlib import Path
import pandas as pd

# ==============================
# CONFIG
# ==============================

BASE_DIR = Path.cwd()

FILE_PATH = BASE_DIR / "data" / "gold" / "cnes_proc.parquet"

THRESHOLD = 0.7  

# ==============================
# TESTE
# ==============================

def test_variacao_mensal_cnes():
    """
    Verifica variação de volume mensal por CNES.
    Cria flag de outlier (IS_OUTLIER).
    Falha se variação > THRESHOLD.
    """

    assert FILE_PATH.exists(), f"Arquivo não encontrado: {FILE_PATH}"

    df = pd.read_parquet(FILE_PATH)

    threshold_pct = THRESHOLD * 100

    # ==============================
    # PASSO 1: AGRUPAR
    # ==============================

    df_grouped = (
        df.groupby(["CNES", "MES"])
        .size()
        .reset_index(name="QTD_LINHAS")
    )

    # ==============================
    # PASSO 2: ORDENAR
    # ==============================

    df_grouped = df_grouped.sort_values(["CNES", "MES"])

    # ==============================
    # PASSO 3: CALCULAR VARIAÇÃO
    # ==============================

    df_grouped["QTD_ANTERIOR"] = (
        df_grouped.groupby("CNES")["QTD_LINHAS"].shift(1)
    )

    df_grouped["VARIACAO"] = (
        (df_grouped["QTD_LINHAS"] - df_grouped["QTD_ANTERIOR"])
        / df_grouped["QTD_ANTERIOR"]
    )

    # ==============================
    # FLAG DE OUTLIER
    # ==============================

    df_grouped["IS_OUTLIER"] = (
        (df_grouped["VARIACAO"].abs() > THRESHOLD)
        & (df_grouped["QTD_ANTERIOR"].notna())
    )

    # ==============================
    # DEBUG
    # ==============================

    print(f"\n📊 DATAFRAME COMPLETO (threshold: {threshold_pct:.0f}%)")
    print(df_grouped)

    # ==============================
    # FILTRAR OUTLIERS
    # ==============================

    df_erro = df_grouped[df_grouped["IS_OUTLIER"]]

    # ==============================
    # OUTPUT
    # ==============================

    if not df_erro.empty:
        print(f"\n❌ Anomalias detectadas (variação > {threshold_pct:.0f}%)")

        print("\n🔍 Amostra:")
        print(
            df_erro[
                [
                    "CNES",
                    "MES",
                    "QTD_LINHAS",
                    "QTD_ANTERIOR",
                    "VARIACAO",
                    "IS_OUTLIER"
                ]
            ].head(10)
        )
    
    else:
        print("\n✅ Número de linhas mensal por CNES está dentro do limite esperado")

    # ==============================
    # ASSERT
    # ==============================

    assert df_erro.empty, (
        f"\nForam encontradas {df_erro.shape[0]} variações acima de {threshold_pct:.0f}%"
    )

# poetry run pytest -s tests/test_variacao_mensal.py # o -s é para mostrar os prints no console
