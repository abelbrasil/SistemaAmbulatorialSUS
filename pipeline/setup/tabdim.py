from pathlib import Path
from datetime import datetime
import pandas as pd
from dbfread import DBF

# ==============================
# CONFIGURAÇÃO
# ==============================

BASE_DIR = Path.cwd()

DBF_DIR = BASE_DIR / "data" / "arq_aux" / "tmp_siadef" / "DBF"
OUTPUT_DIR = BASE_DIR / "data" / "arq_aux" / "tab_dim"

# 🔥 Arquivos a serem processados
FILES = [
    "TB_SIGTAW.dbf",
    "CADGERCE.dbf"
]

# ==============================
# LOGGER
# ==============================

def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ==============================
# PROCESSAR ARQUIVO
# ==============================

def process_file(file_name: str):
    input_file = DBF_DIR / file_name
    output_file = OUTPUT_DIR / file_name.replace(".dbf", ".parquet")

    if not input_file.exists():
        log(f"[ERRO] Arquivo não encontrado: {input_file}")
        return

    log(f"📥 Lendo DBF: {file_name}")

    table = DBF(
        str(input_file),
        encoding="latin1",  # obrigatório para DATASUS
        load=True
    )

    df = pd.DataFrame(iter(table))

    log(f"[OK] {file_name}: {df.shape[0]} linhas carregadas")

    log(f"💾 Salvando: {output_file}")

    df.to_parquet(
        output_file,
        index=False,
        engine="pyarrow"
    )

    log(f"[OK] {file_name} convertido para Parquet")


# ==============================
# PIPELINE PRINCIPAL
# ==============================

def run_tabdim():
    log("=== INÍCIO TAB_DIM (DBF → PARQUET) ===")

    if not DBF_DIR.exists():
        raise FileNotFoundError(
            f"Pasta DBF não encontrada: {DBF_DIR}\n"
            "Execute primeiro o script siadef.py"
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for file_name in FILES:
        process_file(file_name)

    log("=== FIM TAB_DIM ===")


# ==============================
# EXECUÇÃO
# ==============================

if __name__ == "__main__":
    run_tabdim()