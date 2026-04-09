from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from pathlib import Path
import os

# ==============================
# CONFIG
# ==============================

BASE_DIR = Path.cwd()

DB_TYPE = os.getenv("DB_TYPE", "sqlite")  # sqlite | postgres

# ==============================
# DATABASE URL
# ==============================

if DB_TYPE == "sqlite":

    DB_PATH = BASE_DIR / "data" / "db" / "indicadores.db"

    DATABASE_URL = f"sqlite:///{DB_PATH}"

    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False}
    )

else:
    # 🔥 POSTGRES (Docker futuro)
    DATABASE_URL = (
        f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'postgres')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'siasus')}"
    )

    engine = create_engine(DATABASE_URL)

# ==============================
# SESSION
# ==============================

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# ==============================
# BASE
# ==============================

Base = declarative_base()

# ==============================
# DEPENDÊNCIA
# ==============================

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()