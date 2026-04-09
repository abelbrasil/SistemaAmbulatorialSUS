from pathlib import Path
from datetime import datetime
import pandas as pd
from dbfread import DBF

# ==============================
# PARAMETRO (ALTERE AQUI)
# ==============================

#FILE_NAME = "TB_SIGTAW.dbf"
FILE_NAME = "CADGERCE.dbf"

# ==============================
# CONFIGURAÇÃO
# ==============================

BASE_DIR = Path(__file__).resolve().parents[1]

DBF_DIR = BASE_DIR / "data" / "arq_aux" / "tmp_siadef" / "DBF"
OUTPUT_DIR = BASE_DIR / "data" / "arq_aux" / "tab_dim"

INPUT_FILE = DBF_DIR / FILE_NAME
OUTPUT_FILE = OUTPUT_DIR / FILE_NAME.replace(".dbf", ".parquet")

# ==============================
# LOGGER
# ==============================

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ==============================
# PROCESSAMENTO
# ==============================

def run():
    log("=== DBF → PARQUET ===")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {INPUT_FILE}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log(f"Lendo DBF: {INPUT_FILE}")

    table = DBF(
        str(INPUT_FILE),
        encoding="latin1",   # 🔥 obrigatório para DATASUS
        load=True
    )

    df = pd.DataFrame(iter(table))

    log(f"[OK] {df.shape[0]} linhas carregadas")

    log(f"Salvando em: {OUTPUT_FILE}")

    df.to_parquet(
        OUTPUT_FILE,
        index=False,
        engine="pyarrow"
    )

    log("[OK] Conversão concluída")


if __name__ == "__main__":
    run()