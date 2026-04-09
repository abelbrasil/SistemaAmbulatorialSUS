from pathlib import Path
from datetime import datetime
import pandas as pd

# ==============================
# CONFIGURAÇÃO BASE
# ==============================

BASE_DIR = Path.cwd()

INPUT_FILE = BASE_DIR / "data" / "silver" / "PASIA.parquet"
OUTPUT_DIR = BASE_DIR / "data" / "gold"

SIGTAP_FILE = BASE_DIR / "data" / "arq_aux" / "tab_dim" / "TB_SIGTAW.parquet"
CNES_FILE = BASE_DIR / "data" / "arq_aux" / "tab_dim" / "CADGERCE.parquet"

# ==============================
# LOGGER
# ==============================

def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# ==============================
# PIPELINE GOLD
# ==============================

def run_gold():
    log("=== INÍCIO CAMADA GOLD ===")

    # ==============================
    # VALIDAÇÃO
    # ==============================

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {INPUT_FILE}")

    if not SIGTAP_FILE.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {SIGTAP_FILE}")

    if not CNES_FILE.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {CNES_FILE}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ==============================
    # LEITURA
    # ==============================

    log("Lendo arquivo PASIA...")
    df = pd.read_parquet(INPUT_FILE)

    # ==============================
    # TIPOS
    # ==============================

    df["PA_QTDAPR"] = pd.to_numeric(df["PA_QTDAPR"], errors="coerce").fillna(0)
    df["PA_VALAPR"] = pd.to_numeric(df["PA_VALAPR"], errors="coerce").fillna(0)

    # ==============================
    # AGREGAÇÕES
    # ==============================

    log("Calculando agregações...")

    # 🔹 ANO / MÊS
    ano_mes = df.groupby(
        ["ANO", "PA_MVM"], dropna=False
    )[["PA_QTDAPR", "PA_VALAPR"]].sum().reset_index()

    ano_mes.rename(columns={
        "PA_MVM": "MES",
        "PA_QTDAPR": "QTD_APROVADA",
        "PA_VALAPR": "VALOR_APROVADO"
    }, inplace=True)

    # 🔹 CNES / ANO / MÊS
    cnes = df.groupby(
        ["PA_CODUNI", "ANO", "PA_MVM"], dropna=False
    )[["PA_QTDAPR", "PA_VALAPR"]].sum().reset_index()

    cnes.rename(columns={
        "PA_CODUNI": "CNES",
        "PA_MVM": "MES",
        "PA_QTDAPR": "QTD_APROVADA",
        "PA_VALAPR": "VALOR_APROVADO"
    }, inplace=True)

    # 🔹 CNES / PROC / ANO / MÊS (FATO PRINCIPAL)
    cnes_proc = df.groupby(
        ["PA_CODUNI", "PA_PROC_ID", "ANO", "PA_MVM"], dropna=False
    )[["PA_QTDAPR", "PA_VALAPR"]].sum().reset_index()

    cnes_proc.rename(columns={
        "PA_CODUNI": "CNES",
        "PA_PROC_ID": "PROC",
        "PA_MVM": "MES",
        "PA_QTDAPR": "QTD_APROVADA",
        "PA_VALAPR": "VALOR_APROVADO"
    }, inplace=True)

    # ==============================
    # DIMENSÃO PROCEDIMENTO
    # ==============================

    log("Gerando dim_proc...")

    dim_proc = pd.read_parquet(SIGTAP_FILE)

    dim_proc["IP_COD"] = dim_proc["IP_COD"].astype(str)
    cnes_proc["PROC"] = cnes_proc["PROC"].astype(str)

    dim_proc = dim_proc[
        dim_proc["IP_COD"].isin(cnes_proc["PROC"].unique())
    ]

    log(f"[DIM_PROC] {dim_proc.shape[0]} registros")

    # ==============================
    # DIMENSÃO CNES
    # ==============================

    log("Gerando dim_cnes...")

    dim_cnes = pd.read_parquet(CNES_FILE)

    dim_cnes["CNES"] = dim_cnes["CNES"].astype(str)
    cnes["CNES"] = cnes["CNES"].astype(str)

    dim_cnes = dim_cnes[
        dim_cnes["CNES"].isin(cnes["CNES"].unique())
    ]

    dim_cnes = dim_cnes[["CNES", "FANTASIA"]].drop_duplicates()

    dim_cnes.rename(columns={
        "CNES": "CNES_COD",
        "FANTASIA": "ESTABELECIMENTO"
    }, inplace=True)

    log(f"[DIM_CNES] {dim_cnes.shape[0]} registros")

    # ==============================
    # DIMENSÃO CALENDÁRIO
    # ==============================

    log("Gerando dim_calendario...")

    dim_cal = ano_mes[["ANO", "MES"]].drop_duplicates().copy()

    dim_cal["MES"] = dim_cal["MES"].astype(str)
    dim_cal["ANO_NUM"] = dim_cal["ANO"].astype(int)
    dim_cal["MES_NUM"] = dim_cal["MES"].str[4:6].astype(int)

    mapa_meses = {
        1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr",
        5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
        9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
    }

    dim_cal["MES_NOME"] = dim_cal["MES_NUM"].map(mapa_meses)

    dim_cal["MES_ANO_LABEL"] = (
        dim_cal["MES_NOME"] + "/" + dim_cal["ANO"].str[2:]
    )

    dim_cal["ANO_MES"] = dim_cal["MES"]

    dim_cal = dim_cal.sort_values("ANO_MES")

    log(f"[DIM_CAL] {dim_cal.shape[0]} registros")

    # ==============================
    # SALVAR
    # ==============================

    log("Salvando arquivos GOLD...")

    ano_mes.to_parquet(OUTPUT_DIR / "ano_mes.parquet", index=False)
    cnes.to_parquet(OUTPUT_DIR / "cnes.parquet", index=False)
    cnes_proc.to_parquet(OUTPUT_DIR / "cnes_proc.parquet", index=False)

    dim_proc.to_parquet(OUTPUT_DIR / "dim_proc.parquet", index=False)
    dim_cal.to_parquet(OUTPUT_DIR / "dim_calendario.parquet", index=False)
    dim_cnes.to_parquet(OUTPUT_DIR / "dim_cnes.parquet", index=False)

    log("[OK] GOLD gerado com sucesso")
    log("=== FIM CAMADA GOLD ===")


# ==============================
# EXECUÇÃO LOCAL
# ==============================

if __name__ == "__main__":
    run_gold()