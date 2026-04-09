from datetime import datetime
from backend.database import engine, Base

from backend import models  # garante registro no metadata


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def create_schema():
    log("=== INÍCIO CRIAÇÃO SCHEMA ===")

    Base.metadata.create_all(bind=engine)

    log("[OK] Tabelas criadas com sucesso")
    log("=== FIM SCHEMA ===")


if __name__ == "__main__":
    create_schema()