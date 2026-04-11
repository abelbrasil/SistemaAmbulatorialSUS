from pathlib import Path
import pandas as pd

# ==============================
# CONFIG
# ==============================

BASE_DIR = Path.cwd()

# 🔥 AGORA USANDO SILVER (BASE GRANULAR)
PASIA_FILE = BASE_DIR / "data" / "silver" / "PASIA.parquet"
CAL_FILE = BASE_DIR / "data" / "gold" / "dim_calendario.parquet"

# ==============================
# TESTE
# ==============================

def test_completude_temporal_por_cnes():
    """
    Verifica se todos os CNES possuem todos os meses da dimensão calendário.

    Base:
        PASIA (dados granulares)

    Saídas:
        - DataFrame base: CNES x MES existentes
        - DataFrame faltantes: CNES x MES ausentes
    """

    # ==============================
    # VALIDAÇÃO
    # ==============================

    assert PASIA_FILE.exists(), f"Arquivo não encontrado: {PASIA_FILE}"
    assert CAL_FILE.exists(), f"Arquivo não encontrado: {CAL_FILE}"

    df = pd.read_parquet(PASIA_FILE)
    df_cal = pd.read_parquet(CAL_FILE)

    print("\n📊 TOTAL REGISTROS BASE:", df.shape[0])

    # ==============================
    # NORMALIZAÇÃO
    # ==============================

    df["PA_CODUNI"] = df["PA_CODUNI"].astype(str)
    df["PA_MVM"] = df["PA_MVM"].astype(str)

    # ==============================
    # BASE ANALISADA (CNES x MES)
    # ==============================

    df_base = (
        df[["PA_CODUNI", "PA_MVM"]]
        .drop_duplicates()
        .rename(columns={
            "PA_CODUNI": "CNES",
            "PA_MVM": "MES"
        })
        .sort_values(["CNES", "MES"])
        .reset_index(drop=True)
    )

    print("\n📊 DATAFRAME BASE (CNES x MES EXISTENTE):")
    print(df_base)

    # ==============================
    # CONJUNTOS
    # ==============================

    cnes_list = df_base["CNES"].unique()
    meses_esperados = df_cal["ANO_MES"].astype(str).unique()

    # ==============================
    # GERAR FALTANTES
    # ==============================

    registros_faltantes = []

    for cnes in cnes_list:
        meses_cnes = df_base[df_base["CNES"] == cnes]["MES"].unique()

        missing = set(meses_esperados) - set(meses_cnes)

        for mes in missing:
            registros_faltantes.append({
                "CNES": cnes,
                "MES": mes
            })

    # ==============================
    # DATAFRAME FALTANTES
    # ==============================

    df_faltantes = pd.DataFrame(registros_faltantes)

    if not df_faltantes.empty:
        df_faltantes = (
            df_faltantes
            .sort_values(["CNES", "MES"])
            .reset_index(drop=True)
        )

    # ==============================
    # OUTPUT
    # ==============================

    print("\n📅 TOTAL MESES ESPERADOS:", len(meses_esperados))
    print("🏥 TOTAL CNES:", len(cnes_list))

    if not df_faltantes.empty:
        print("\n❌ DATAFRAME DE FALTANTES (CNES x MES):")
        print(df_faltantes)

        resumo = (
            df_faltantes.groupby("CNES")
            .size()
            .reset_index(name="QTD_MESES_FALTANTES")
            .sort_values("QTD_MESES_FALTANTES", ascending=False)
        )

        print("\n📌 RESUMO POR CNES:")
        print(resumo)

    else:
        print("\n✅ Todos os CNES possuem todos os meses")

    # ==============================
    # ASSERT
    # ==============================

    assert df_faltantes.empty, (
        f"\nForam encontrados {df_faltantes.shape[0]} registros faltantes (CNES x MES)"
    )
    
#  poetry run pytest -s tests/test_completude_mensal.py # o -s é para mostrar os prints no console
