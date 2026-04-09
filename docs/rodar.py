from pathlib import Path

# ==============================
# CONFIGURAÇÃO
# ==============================

BASE_DIR = Path(__file__).resolve().parents[1]
print(BASE_DIR)

BASE_DIR = Path.cwd()  # 🔥 pega onde você está executando
print(BASE_DIR)