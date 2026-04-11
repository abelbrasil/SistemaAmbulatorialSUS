from pathlib import Path
import pandas as pd

# ==============================
# CONFIG
# ==============================

BASE_DIR = Path.cwd()

# 🔥 AGORA USANDO SILVER (GRANULAR)
FILE_PATH = BASE_DIR / "data" / "silver" / "PASIA.parquet"

THRESHOLD = 0.5  # 50%

# ==============================
# TESTE
# ==============================

def test_variacao_mensal_cnes():
    """
    Verifica variação de volume mensal por CNES com base no dataset granular (PASIA).

    Detecta variações anormais no número de registros (linhas).
    Falha se variação > THRESHOLD.
    """

    assert FILE_PATH.exists(), f"Arquivo não encontrado: {FILE_PATH}"

    df = pd.read_parquet(FILE_PATH)

    threshold_pct = THRESHOLD * 100

    print("\n📊 TOTAL REGISTROS BASE:", df.shape[0])

    # ==============================
    # 🔥 GARANTIR TIPOS
    # ==============================

    df["PA_CODUNI"] = df["PA_CODUNI"].astype(str)
    df["PA_MVM"] = df["PA_MVM"].astype(str)

    # ==============================
    # PASSO 1: AGRUPAR (VOLUME REAL)
    # ==============================

    df_grouped = (
        df.groupby(["PA_CODUNI", "PA_MVM"])
        .size()
        .reset_index(name="QTD_LINHAS")
        .rename(columns={
            "PA_CODUNI": "CNES",
            "PA_MVM": "MES"
        })
    )

    # ==============================
    # PASSO 2: ORDENAR
    # ==============================

    df_grouped = df_grouped.sort_values(["CNES", "MES"])

    # ==============================
    # PASSO 3: VARIAÇÃO MENSAL
    # ==============================

    df_grouped["QTD_ANTERIOR"] = (
        df_grouped.groupby("CNES")["QTD_LINHAS"].shift(1)
    )

    df_grouped["VARIACAO"] = (
        (df_grouped["QTD_LINHAS"] - df_grouped["QTD_ANTERIOR"])
        / df_grouped["QTD_ANTERIOR"]
    )

    # ==============================
    # PASSO 4: FLAG OUTLIER
    # ==============================

    df_grouped["IS_OUTLIER"] = (
        (df_grouped["VARIACAO"].abs() > THRESHOLD)
        & (df_grouped["QTD_ANTERIOR"].notna())
    )

    # ==============================
    # DEBUG COMPLETO
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

        print("\n🔍 OUTLIERS DETECTADOS:")
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
            ]
        )

        # 🔥 RESUMO
        resumo = (
            df_erro.groupby("CNES")
            .size()
            .reset_index(name="QTD_OUTLIERS")
            .sort_values("QTD_OUTLIERS", ascending=False)
        )

        print("\n📌 RESUMO POR CNES:")
        print(resumo)

    else:
        print("\n✅ Volume mensal por CNES está dentro do limite esperado")

    # ==============================
    # ASSERT
    # ==============================

    assert df_erro.empty, (
        f"\nForam encontradas {df_erro.shape[0]} variações acima de {threshold_pct:.0f}%"
    )

# poetry run pytest -s tests/test_variacao_mensal.py # o -s é para mostrar os prints no console
