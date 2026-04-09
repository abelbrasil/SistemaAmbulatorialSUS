from pathlib import Path
from datetime import datetime
from ftplib import FTP
import zipfile

# ==============================
# CONFIGURAÇÃO
# ==============================

BASE_DIR = Path.cwd()

TMP_DIR = BASE_DIR / "data" / "arq_aux" / "tmp_siadef"

FTP_HOST = "ftp.datasus.gov.br"
FTP_PATH = "/dissemin/publicos/SIASUS/200801_/Auxiliar"
FILE_NAME = "TAB_SIA.zip"

ZIP_FILE = TMP_DIR / FILE_NAME

# ==============================
# LOGGER
# ==============================

def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ==============================
# DOWNLOAD FTP
# ==============================

def download_ftp():
    log("🌐 Conectando ao FTP...")

    ftp = FTP(FTP_HOST)
    ftp.login()
    ftp.cwd(FTP_PATH)

    log("⬇️ Baixando arquivo...")

    with open(ZIP_FILE, "wb") as f:
        ftp.retrbinary(f"RETR {FILE_NAME}", f.write)

    ftp.quit()

    log(f"[OK] Download concluído: {ZIP_FILE}")


# ==============================
# EXTRAÇÃO
# ==============================

def extract_zip():
    log("📦 Extraindo ZIP...")

    with zipfile.ZipFile(ZIP_FILE, "r") as zip_ref:
        zip_ref.extractall(TMP_DIR)

    log("[OK] Extração concluída")


# ==============================
# PIPELINE PRINCIPAL
# ==============================

def run_siadef():
    log("=== INÍCIO DOWNLOAD SIA AUX ===")

    TMP_DIR.mkdir(parents=True, exist_ok=True)

    download_ftp()
    extract_zip()

    log("=== FIM SIA AUX ===")


# ==============================
# EXECUÇÃO
# ==============================

if __name__ == "__main__":
    run_siadef()