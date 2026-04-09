from pathlib import Path
import pandas as pd

# ==============================
# CONFIG
# ==============================

BASE_DIR = Path(__file__).resolve().parents[1]
FILE_PATH = BASE_DIR / "data" / "gold" / "cnes_proc.parquet"

# ==============================
# TESTE
# ==============================

def test_outliers_qtd_aprovada_por_cnes():
    """
    Detecta outliers na QTD_APROVADA por CNES,
    considerando média + desvio padrão.
    Exibe dataframe completo com flag de outlier.
    """

    assert FILE_PATH.exists(), f"Arquivo não encontrado: {FILE_PATH}"

    df = pd.read_parquet(FILE_PATH)

    print("\n📊 TOTAL REGISTROS:", df.shape[0])

    # ==============================
    # AGREGAÇÃO CNES + MES
    # ==============================

    df_agg = (
        df.groupby(["CNES", "MES"], as_index=False)
        .agg({
            "QTD_APROVADA": "sum"
        })
        .sort_values(["CNES", "MES"])
    )

    # ==============================
    # MÉDIA E DESVIO POR CNES
    # ==============================

    stats = (
        df_agg.groupby("CNES")["QTD_APROVADA"]
        .agg(["mean", "std"])
        .reset_index()
    )

    stats.rename(columns={
        "mean": "MEDIA",
        "std": "STD"
    }, inplace=True)

    df_agg = df_agg.merge(stats, on="CNES", how="left")

    # ==============================
    # DETECTAR OUTLIERS
    # ==============================

    df_agg["LIMITE"] = df_agg["MEDIA"] + df_agg["STD"]

    df_agg["IS_OUTLIER"] = df_agg["QTD_APROVADA"] > df_agg["LIMITE"]

    # ==============================
    # PRINT DATAFRAME COMPLETO
    # ==============================

    print("\n📊 DATAFRAME COMPLETO COM FLAG DE OUTLIER:")
    print(df_agg)

    # ==============================
    # FILTRAR OUTLIERS
    # ==============================

    df_outliers = df_agg[df_agg["IS_OUTLIER"]]

    print("\n🚨 TOTAL OUTLIERS:", df_outliers.shape[0])

    if not df_outliers.empty:
        print("\n🔍 OUTLIERS DETECTADOS:")
        print(
            df_outliers[
                ["CNES", "MES", "QTD_APROVADA", "MEDIA", "STD", "LIMITE"]
            ]
        )

    # ==============================
    # ASSERT
    # ==============================

    assert df_outliers.empty, (
        f"Foram encontrados {df_outliers.shape[0]} outliers "
        f"(QTD_APROVADA > média + desvio padrão)"
    )

# poetry run pytest -s tests/test_quality_numbers.py