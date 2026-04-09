from pathlib import Path
from datetime import datetime
import pandas as pd

from backend.database import SessionLocal
from backend.models import (
    TabDimProcedimento,
    TabDimCnes,
    TabDimCalendario,
    TabFato
)

# ==============================
# CONFIGURAÇÃO
# ==============================

BASE_DIR = Path.cwd()
GOLD_DIR = BASE_DIR / "data" / "gold"

DIM_PROC_FILE = GOLD_DIR / "dim_proc.parquet"
DIM_CNES_FILE = GOLD_DIR / "dim_cnes.parquet"
DIM_CAL_FILE = GOLD_DIR / "dim_calendario.parquet"
FATO_FILE = GOLD_DIR / "cnes_proc.parquet"

# ==============================
# LOGGER
# ==============================

def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# ==============================
# LEITURA
# ==============================

def load_dim_proc():
    df = pd.read_parquet(DIM_PROC_FILE)
    df["IP_COD"] = df["IP_COD"].astype(str)
    return df


def load_dim_cnes():
    df = pd.read_parquet(DIM_CNES_FILE)
    df["CNES_COD"] = df["CNES_COD"].astype(str)
    return df


def load_dim_cal():
    df = pd.read_parquet(DIM_CAL_FILE)
    df["ANO_MES"] = df["ANO_MES"].astype(str)
    return df


def load_fato():
    df = pd.read_parquet(FATO_FILE)

    df["CNES"] = df["CNES"].astype(str)
    df["PROC"] = df["PROC"].astype(str)
    df["ANO"] = df["ANO"].astype(str)
    df["MES"] = df["MES"].astype(str)

    return df


# ==============================
# INSERT DIMENSÕES
# ==============================

def insert_dim_proc(df):
    with SessionLocal() as db:
        log("Limpando dim_procedimento...")
        db.query(TabDimProcedimento).delete()
        db.commit()

        records = [
            TabDimProcedimento(
                procedimento=row["IP_COD"],
                descricao=row.get("IP_DSCR")
            )
            for _, row in df.iterrows()
        ]

        db.add_all(records)
        db.commit()

    log(f"[OK] dim_procedimento: {len(records)} registros")


def insert_dim_cnes(df):
    with SessionLocal() as db:
        log("Limpando dim_cnes...")
        db.query(TabDimCnes).delete()
        db.commit()

        records = [
            TabDimCnes(
                cnes=row["CNES_COD"],
                estabelecimento=row["ESTABELECIMENTO"]
            )
            for _, row in df.iterrows()
        ]

        db.add_all(records)
        db.commit()

    log(f"[OK] dim_cnes: {len(records)} registros")


def insert_dim_cal(df):
    with SessionLocal() as db:
        log("Limpando dim_calendario...")
        db.query(TabDimCalendario).delete()
        db.commit()

        records = [
            TabDimCalendario(
                anomes=row["ANO_MES"],
                ano=row["ANO"],
                mes_num=int(row["MES_NUM"]),
                mes_nome=row["MES_NOME"],
                mes_ano_label=row["MES_ANO_LABEL"]
            )
            for _, row in df.iterrows()
        ]

        db.add_all(records)
        db.commit()

    log(f"[OK] dim_calendario: {len(records)} registros")


# ==============================
# INSERT FATO
# ==============================

def insert_fato(df, dim_proc_df, dim_cnes_df, dim_cal_df):
    with SessionLocal() as db:
        log("Limpando fato_metrica...")
        db.query(TabFato).delete()
        db.commit()

        # 🔥 VALIDAÇÃO FK
        valid_proc = set(dim_proc_df["IP_COD"])
        valid_cnes = set(dim_cnes_df["CNES_COD"])
        valid_cal = set(dim_cal_df["ANO_MES"])

        df = df[
            (df["PROC"].isin(valid_proc)) &
            (df["CNES"].isin(valid_cnes)) &
            (df["MES"].isin(valid_cal))
        ]

        log(f"[INFO] Registros válidos após FK: {df.shape[0]}")

        records = [
            TabFato(
                cnes=row["CNES"],
                procedimento=row["PROC"],
                anomes=row["MES"],
                ano=row["ANO"],
                quantidade=int(row["QTD_APROVADA"]),
                valor_aprovado=float(row["VALOR_APROVADO"])
            )
            for _, row in df.iterrows()
        ]

        db.add_all(records)
        db.commit()

    log(f"[OK] fato_metrica: {len(records)} registros")


# ==============================
# PIPELINE
# ==============================

def run_loader():
    log("=== INÍCIO DATA LOADER ===")

    dim_proc_df = load_dim_proc()
    dim_cnes_df = load_dim_cnes()
    dim_cal_df = load_dim_cal()
    fato_df = load_fato()

    log("🔹 Carregando dimensões...")
    insert_dim_proc(dim_proc_df)
    insert_dim_cnes(dim_cnes_df)
    insert_dim_cal(dim_cal_df)

    log("🔹 Carregando fato...")
    insert_fato(fato_df, dim_proc_df, dim_cnes_df, dim_cal_df)

    log("=== FIM DATA LOADER ===")


# ==============================
# EXECUÇÃO
# ==============================

if __name__ == "__main__":
    run_loader()