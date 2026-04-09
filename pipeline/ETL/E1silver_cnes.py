from pathlib import Path
from datetime import datetime
import pandas as pd
import re

# ==============================
# CONFIGURAÇÃO BASE
# ==============================

BASE_DIR = Path.cwd()

BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver_cnes"

# 🔥 CONTROLE ENTRE CAMADAS
CONTROL_FILE = SILVER_DIR / "_control_processed.txt"

# ==============================
# LOGGER
# ==============================

def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ==============================
# CONTROLE
# ==============================

def load_control():
    if not CONTROL_FILE.exists():
        return set()

    with open(CONTROL_FILE, "r") as f:
        return set(line.strip() for line in f.readlines())


def save_control(file_name):
    processed = load_control()

    if file_name not in processed:
        with open(CONTROL_FILE, "a") as f:
            f.write(file_name + "\n")


# ==============================
# UTIL
# ==============================

def extrair_ano(nome_arquivo: str):
    try:
        match = re.search(r'PACE(\d{2})\d{2}', nome_arquivo.upper())
        if match:
            return f"20{match.group(1)}"
        return None
    except:
        return None


# ==============================
# PROCESSAMENTO
# ==============================

def process_file(file_path: Path, cod_unidades: list[str]):
    log(f"[PROCESSANDO] {file_path.name}")

    try:
        df = pd.read_parquet(file_path)

        if "PA_CODUNI" not in df.columns:
            log(f"[AVISO] Coluna PA_CODUNI não encontrada em {file_path.name}")
            return

        df_filtered = df[df["PA_CODUNI"].astype(str).isin(cod_unidades)]

        log(f"[FILTRADO] {df_filtered.shape[0]} linhas")

        output_file = SILVER_DIR / file_path.name

        df_filtered.to_parquet(
            output_file,
            index=False,
            engine="pyarrow"
        )

        log(f"[OK] {output_file}")

        save_control(file_path.name)

    except Exception as e:
        log(f"[ERRO] {file_path.name}: {e}")


# ==============================
# SELEÇÃO DE ARQUIVOS
# ==============================

def selecionar_arquivos(rodar_tudo: bool, processar_ano):
    bronze_files = list(BRONZE_DIR.glob("*.parquet"))

    if not bronze_files:
        raise Exception("Nenhum arquivo encontrado na Bronze")

    log(f"{len(bronze_files)} arquivos na Bronze")

    # 🔥 PRIORIDADE 1 → RODAR TUDO
    if rodar_tudo:
        log("[MODO] Reprocessamento total ativado")
        return bronze_files

    # 🔥 PRIORIDADE 2 → PROCESSAR ANO
    if processar_ano:
        ano_param = str(processar_ano)

        log(f"[MODO] Reprocessamento do ano {ano_param}")

        arquivos_ano = [
            f for f in bronze_files
            if extrair_ano(f.name) == ano_param
        ]

        log(f"{len(arquivos_ano)} arquivos do ano {ano_param}")

        return arquivos_ano

    # 🔥 PADRÃO → INCREMENTAL
    silver_files = {f.name for f in SILVER_DIR.glob("*.parquet")}

    novos = [
        f for f in bronze_files
        if f.name not in silver_files
    ]

    log(f"{len(novos)} arquivos novos")

    return novos


# ==============================
# PIPELINE PRINCIPAL
# ==============================

def run_silver_cnes(
    cod_unidades: list[str],
    rodar_tudo: bool = False,
    processar_ano: str | int | None = None
):
    log("=== INÍCIO CAMADA SILVER (CNES) ===")

    if not BRONZE_DIR.exists():
        raise FileNotFoundError(f"Pasta Bronze não encontrada: {BRONZE_DIR}")

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    arquivos_para_processar = selecionar_arquivos(
        rodar_tudo,
        processar_ano
    )

    if not arquivos_para_processar:
        log("Nada para processar ✅")
        return

    for file in arquivos_para_processar:
        process_file(file, cod_unidades)

    log("=== FIM CAMADA SILVER (CNES) ===")


# ==============================
# EXECUÇÃO LOCAL (OPCIONAL)
# ==============================

if __name__ == "__main__":
    run_silver_cnes(
        cod_unidades=["2561492", "2481286"],
        rodar_tudo=False,
        processar_ano=None
    )