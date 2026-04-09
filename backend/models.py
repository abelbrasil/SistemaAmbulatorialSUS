from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.sql import func
from backend.database import Base

# ==============================
# DIMENSÃO: PROCEDIMENTO
# ==============================

class TabDimProcedimento(Base):
    __tablename__ = "dim_procedimento"

    procedimento = Column(String, primary_key=True, index=True)
    descricao = Column(String)


# ==============================
# DIMENSÃO: CNES
# ==============================

class TabDimCnes(Base):
    __tablename__ = "dim_cnes"

    cnes = Column(String, primary_key=True, index=True)
    estabelecimento = Column(String)


# ==============================
# DIMENSÃO: CALENDÁRIO
# ==============================

class TabDimCalendario(Base):
    __tablename__ = "dim_calendario"

    anomes = Column(String, primary_key=True, index=True)
    ano = Column(String)
    mes_num = Column(Integer)
    mes_nome = Column(String)
    mes_ano_label = Column(String)


# ==============================
# FATO
# ==============================

class TabFato(Base):
    __tablename__ = "fato_metrica"

    id = Column(Integer, primary_key=True, index=True)

    # 🔗 CHAVES ESTRANGEIRAS
    cnes = Column(String, ForeignKey("dim_cnes.cnes"), index=True)
    procedimento = Column(String, ForeignKey("dim_procedimento.procedimento"), index=True)
    anomes = Column(String, ForeignKey("dim_calendario.anomes"), index=True)

    # 🔹 atributos
    ano = Column(String, index=True)

    # 🔹 métricas
    quantidade = Column(Integer)
    valor_aprovado = Column(Float)

    # 🔹 controle
    created_at = Column(DateTime(timezone=True), default=func.now(), index=True)