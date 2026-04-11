from pathlib import Path
import pandas as pd

# ==============================
# CONFIG
# ==============================

BASE_DIR = Path(__file__).resolve().parents[1]
FILE_PATH = BASE_DIR / "data" / "gold" / "cnes_proc.parquet"

THRESHOLD = 2  # 🔥 multiplicador do desvio padrão

# ==============================
# TESTE
# ==============================

def test_outliers_qtd_aprovada_por_cnes_ano():
    """
    Detecta outliers na QTD_APROVADA por CNES,
    considerando média e desvio padrão por ANO.

    Regra:
        outlier se QTD_APROVADA > média + (THRESHOLD * std)
    """

    assert FILE_PATH.exists(), f"Arquivo não encontrado: {FILE_PATH}"

    df = pd.read_parquet(FILE_PATH)

    print("\n📊 TOTAL REGISTROS:", df.shape[0])

    # ==============================
    # CRIAR ANO (se não existir)
    # ==============================

    if "ANO" not in df.columns:
        df["ANO"] = df["MES"].astype(str).str[:4]

    # ==============================
    # AGREGAÇÃO CNES + MES
    # ==============================

    df_agg = (
        df.groupby(["CNES", "ANO", "MES"], as_index=False)
        .agg({
            "QTD_APROVADA": "sum"
        })
        .sort_values(["CNES", "ANO", "MES"])
    )

    # ==============================
    # MÉDIA E DESVIO POR CNES + ANO
    # ==============================

    stats = (
        df_agg.groupby(["CNES", "ANO"])["QTD_APROVADA"]
        .agg(["mean", "std"])
        .reset_index()
    )

    stats.rename(columns={
        "mean": "MEDIA_ANUAL",
        "std": "STD_ANUAL"
    }, inplace=True)

    df_agg = df_agg.merge(stats, on=["CNES", "ANO"], how="left")

    # ==============================
    # LIMITE DINÂMICO
    # ==============================

    df_agg["LIMITE"] = df_agg["MEDIA_ANUAL"] + (THRESHOLD * df_agg["STD_ANUAL"])

    # ==============================
    # DETECTAR OUTLIERS
    # ==============================

    df_agg["IS_OUTLIER"] = df_agg["QTD_APROVADA"] > df_agg["LIMITE"]

    # ==============================
    # DEBUG COMPLETO
    # ==============================

    print("\n📊 DATAFRAME COMPLETO (ANUAL):")
    print(df_agg)

    # ==============================
    # FILTRAR OUTLIERS
    # ==============================

    df_outliers = df_agg[df_agg["IS_OUTLIER"]]

    print(f"\n🚨 TOTAL OUTLIERS (threshold={THRESHOLD}):", df_outliers.shape[0])

    if not df_outliers.empty:
        print("\n🔍 OUTLIERS DETECTADOS:")
        print(
            df_outliers[
                [
                    "CNES",
                    "ANO",
                    "MES",
                    "QTD_APROVADA",
                    "MEDIA_ANUAL",
                    "STD_ANUAL",
                    "LIMITE"
                ]
            ]
        )

        # 🔥 RESUMO POR CNES + ANO
        resumo = (
            df_outliers.groupby(["CNES", "ANO"])
            .size()
            .reset_index(name="QTD_OUTLIERS")
            .sort_values("QTD_OUTLIERS", ascending=False)
        )

        print("\n📌 RESUMO POR CNES + ANO:")
        print(resumo)

    else:
        print("\n✅ Nenhum outlier detectado")

    # ==============================
    # ASSERT
    # ==============================

    assert df_outliers.empty, (
        f"Foram encontrados {df_outliers.shape[0]} outliers "
        f"(threshold={THRESHOLD})"
    )

    # poetry run pytest -s tests/test_quality_number_anual.py