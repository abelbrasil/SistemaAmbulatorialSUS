from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List

from backend.database import get_db
from backend.queries import (
    list_fato_detalhado,
    get_analitico,
    get_cnes_list,
    get_procedimentos_list,
    get_calendario_list
)

# ==============================
# CONFIGURAÇÃO
# ==============================

MAX_LIMIT = 1000

# ==============================
# ROUTER
# ==============================

router = APIRouter(
    prefix="/api",
    tags=["Produção SIASUS"]
)

# ==============================
# ROTA 1: DADOS DETALHADOS
# ==============================

@router.get("/dados")
def listar_dados(
    procedimento: Optional[str] = Query(
        None,
        description="Código do procedimento (ex: 0101010028)"
    ),
    cnes: Optional[str] = Query(
        None,
        description="Código CNES (ex: 2561492)"
    ),
    ano: Optional[str] = Query(
        None,
        description="Ano (ex: 2025)"
    ),
    anomes: Optional[str] = Query(
        None,
        description="Ano/Mês (ex: 202511)"
    ),
    limit: int = Query(
        100,
        ge=1,
        le=MAX_LIMIT,
        description="Quantidade de registros por página"
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Deslocamento inicial"
    ),
    db: Session = Depends(get_db)
):
    """
    Retorna dados detalhados do cubo (fato + dimensões)
    """

    total, data = list_fato_detalhado(
        db=db,
        procedimento=procedimento,
        cnes=cnes,
        ano=ano,
        anomes=anomes,
        limit=limit,
        offset=offset
    )

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": data
    }


# ==============================
# ROTA 2: AGREGAÇÃO
# ==============================

@router.get("/analitico")
def analitico(
    cnes: Optional[List[str]] = Query(None),
    procedimento: Optional[List[str]] = Query(
        None,
        description="Código do procedimento (ex: 0101010028, 0301010072)"
    ),
    ano: Optional[List[str]] = Query(None),  
    db: Session = Depends(get_db)
):
    data = get_analitico(db, cnes, procedimento, ano)
    return {
        "total": len(data),
        "data": data
    }

# ==============================
# 🔥 NOVA ROTA 3: LISTAR CNES
# ==============================

@router.get("/cnes")
def listar_cnes(db: Session = Depends(get_db)):
    """
    Retorna lista de estabelecimentos (CNES)
    """

    data = get_cnes_list(db)

    return {
        "total": len(data),
        "data": data
    }


# ==============================
# 🔥 NOVA ROTA 4: LISTAR PROCEDIMENTOS
# ==============================

@router.get("/procedimentos")
def listar_procedimentos(db: Session = Depends(get_db)):
    """
    Retorna lista de procedimentos
    """

    data = get_procedimentos_list(db)

    return {
        "total": len(data),
        "data": data
    }


# ==============================
# 🔥 NOVA ROTA 5: LISTAR CALENDÁRIO
# ==============================

@router.get("/calendario")
def listar_calendario(db: Session = Depends(get_db)):
    """
    Retorna lista de períodos (ano/mês)
    """

    data = get_calendario_list(db)

    return {
        "total": len(data),
        "data": data
    }