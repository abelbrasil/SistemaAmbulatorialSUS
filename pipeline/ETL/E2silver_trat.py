from pathlib import Path
from datetime import datetime
import pandas as pd
import re
import os

# ==============================
# CONFIGURAÇÃO BASE
# ==============================

BASE_DIR = Path.cwd()

INPUT_DIR = BASE_DIR / "data" / "silver_cnes"
OUTPUT_DIR = BASE_DIR / "data" / "silver"
OUTPUT_FILE = OUTPUT_DIR / "PASIA.parquet"

CONTROL_INPUT_FILE = INPUT_DIR / "_control_processed.txt"

# 🔥 COLUNAS DEFINIDAS
COLUNAS = [
    "PA_CODUNI",
    "PA_CMP",
    "PA_MVM",
    "PA_UFMUN",
    "PA_MUNPCN",
    "PA_PROC_ID",
    "PA_CBOCOD",
    "PA_CIDPRI",
    "PA_DOCORIG",
    "PA_TPFIN",
    "PA_QTDPRO",
    "PA_QTDAPR",
    "PA_VALPRO",
    "PA_VALAPR"
]

# ==============================
# LOGGER
# ==============================

def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# ==============================
# UTIL
# ==============================

def extrair_ano(nome_arquivo: str):
    match = re.search(r'PACE(\d{2})\d{2}', nome_arquivo.upper())
    if match:
        return f"20{match.group(1)}"
    return None

# ==============================
# CONTROLE
# ==============================

def load_control_files():
    if not CONTROL_INPUT_FILE.exists():
        return []

    with open(CONTROL_INPUT_FILE, "r") as f:
        files = [line.strip() for line in f.readlines()]

    log(f"{len(files)} arquivos no controle (silver_select)")
    return files

# ==============================
# PROCESSAMENTO
# ==============================

def process_file(file_path: Path):
    log(f"[LENDO] {file_path.name}")

    df = pd.read_parquet(file_path)

    missing_cols = [col for col in COLUNAS if col not in df.columns]
    if missing_cols:
        log(f"[AVISO] Colunas ausentes: {missing_cols}")

    cols_present = [col for col in COLUNAS if col in df.columns]
    return df[cols_present]

# ==============================
# SELEÇÃO DE ARQUIVOS
# ==============================

def selecionar_arquivos(rodar_tudo: bool, processar_ano):
    all_files = list(INPUT_DIR.glob("*.parquet"))

    if not all_files:
        raise Exception("Nenhum arquivo encontrado em silver_cnes")

    # 🔥 PRIORIDADE 1 → RODAR TUDO
    if rodar_tudo:
        log("[MODO] Reprocessamento total")
        return all_files

    # 🔥 PRIORIDADE 2 → PROCESSAR ANO
    if processar_ano:
        ano_param = str(processar_ano)

        arquivos = [
            f for f in all_files
            if extrair_ano(f.name) == ano_param
        ]

        log(f"{len(arquivos)} arquivos do ano {ano_param}")
        return arquivos

    # 🔥 PADRÃO → CONTROLE
    arquivos_controle = load_control_files()

    arquivos = [
        INPUT_DIR / nome for nome in arquivos_controle
        if (INPUT_DIR / nome).exists()
    ]

    log(f"{len(arquivos)} arquivos para processamento incremental")
    return arquivos

# ==============================
# PIPELINE PRINCIPAL
# ==============================

def run_silver_trat(
    rodar_tudo: bool = True,
    processar_ano: str | int | None = None
):
    log("=== INÍCIO CAMADA SILVER_TRAT ===")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    arquivos = selecionar_arquivos(
        rodar_tudo,
        processar_ano
    )

    if not arquivos:
        log("Nada para processar ✅")
        return

    dfs = []

    for file in arquivos:
        try:
            df = process_file(file)
            dfs.append(df)
        except Exception as e:
            log(f"[ERRO] {file.name}: {e}")

    if not dfs:
        log("Nenhum dado processado")
        return

    df_novo = pd.concat(dfs, ignore_index=True)

    log(f"[NOVOS] {df_novo.shape[0]} linhas")

    # ==============================
    # CRIAR ANO
    # ==============================

    if "PA_MVM" in df_novo.columns:
        df_novo["ANO"] = df_novo["PA_MVM"].astype(str).str[:4]

    # ==============================
    # MERGE COM EXISTENTE
    # ==============================

    if OUTPUT_FILE.exists() and not rodar_tudo and not processar_ano:
        log("[APPEND] Carregando base existente")

        df_existente = pd.read_parquet(OUTPUT_FILE)
        df_final = pd.concat([df_existente, df_novo], ignore_index=True)

    else:
        log("[REBUILD] Criando nova base")
        df_final = df_novo

    log(f"[TOTAL FINAL] {df_final.shape[0]} linhas")

    # ==============================
    # SALVAR
    # ==============================

    df_final.to_parquet(
        OUTPUT_FILE,
        index=False,
        engine="pyarrow"
    )

    log(f"[OK] {OUTPUT_FILE}")

    # ==============================
    # LIMPAR CONTROLE
    # ==============================

    if CONTROL_INPUT_FILE.exists():
        os.remove(CONTROL_INPUT_FILE)
        log("[LIMPEZA] Controle removido")

    log("=== FIM CAMADA SILVER_TRAT ===")


# ==============================
# EXECUÇÃO LOCAL
# ==============================

if __name__ == "__main__":
    run_silver_trat(
        rodar_tudo=False,
        processar_ano=None
    )