from sqlalchemy.orm import Session
from sqlalchemy import func

from backend.models import (
    TabFato,
    TabDimProcedimento,
    TabDimCnes,
    TabDimCalendario
)

# ==============================
# QUERY 1: FATO + DIMENSÕES
# ==============================

def list_fato_detalhado(
    db: Session,
    procedimento: str = None,
    cnes: str = None,
    ano: str = None,
    anomes: str = None,
    limit: int = 100,
    offset: int = 0
):

    base_query = (
        db.query(
            TabFato.cnes,
            TabDimCnes.estabelecimento,
            TabFato.procedimento,
            TabDimProcedimento.descricao,
            TabFato.ano,
            TabFato.anomes,
            TabDimCalendario.mes_ano_label,
            TabFato.quantidade,
            TabFato.valor_aprovado
        )
        .outerjoin(
            TabDimProcedimento,
            TabFato.procedimento == TabDimProcedimento.procedimento
        )
        .outerjoin(
            TabDimCnes,
            TabFato.cnes == TabDimCnes.cnes
        )
        .outerjoin(
            TabDimCalendario,
            TabFato.anomes == TabDimCalendario.anomes
        )
    )

    # ==============================
    # FILTROS
    # ==============================

    if procedimento:
        base_query = base_query.filter(TabFato.procedimento == procedimento)

    if cnes:
        base_query = base_query.filter(TabFato.cnes == cnes)

    if ano:
        base_query = base_query.filter(TabFato.ano == ano)

    if anomes:
        base_query = base_query.filter(TabFato.anomes == anomes)

    # ==============================
    # TOTAL
    # ==============================

    total = base_query.count()

    # ==============================
    # PAGINAÇÃO
    # ==============================

    results = (
        base_query
        .order_by(TabFato.cnes, TabFato.anomes)
        .offset(offset)
        .limit(limit)
        .all()
    )

    data = [
        {
            "cnes": r.cnes,
            "estabelecimento": r.estabelecimento,
            "procedimento": r.procedimento,
            "descricao": r.descricao,
            "ano": r.ano,
            "anomes": r.anomes,
            "mes_label": r.mes_ano_label,
            "quantidade": r.quantidade,
            "valor": r.valor_aprovado
        }
        for r in results
    ]

    return total, data


# ==============================
# QUERY 2: AGREGAÇÃO CNES + MÊS
# ==============================

def get_analitico(
    db: Session,
    cnes: list[str] = None,
    procedimento: list[str] = None,
    ano: list[str] = None
):

    query = db.query(
        TabFato.cnes,
        TabDimCnes.estabelecimento,
        TabFato.procedimento,
        TabDimProcedimento.descricao,
        TabFato.ano,
        TabFato.anomes,
        func.sum(TabFato.quantidade).label("quantidade"),
        func.sum(TabFato.valor_aprovado).label("valor")
    ).join(
        TabDimCnes, TabFato.cnes == TabDimCnes.cnes
    ).join(
        TabDimProcedimento, TabFato.procedimento == TabDimProcedimento.procedimento
    )

    # 🔥 FILTROS MULTIPLOS
    if cnes:
        query = query.filter(TabFato.cnes.in_(cnes))

    if procedimento:
        query = query.filter(TabFato.procedimento.in_(procedimento))

    if ano:
        query = query.filter(TabFato.ano.in_(ano))

    query = query.group_by(
        TabFato.cnes,
        TabDimCnes.estabelecimento,
        TabFato.procedimento,
        TabDimProcedimento.descricao,
        TabFato.ano,
        TabFato.anomes
    )

    results = query.all()

    return [
        {
            "cnes": r.cnes,
            "estabelecimento": r.estabelecimento,
            "procedimento": r.procedimento,
            "descricao": r.descricao,
            "ano": r.ano,
            "anomes": r.anomes,
            "quantidade": float(r.quantidade or 0),
            "valor": float(r.valor or 0)
        }
        for r in results
    ]


# ==============================
# ENDPOINTS AUXILIARES
# ==============================

def get_cnes_list(db: Session):
    results = (
        db.query(
            TabDimCnes.cnes,
            TabDimCnes.estabelecimento
        )
        .order_by(TabDimCnes.estabelecimento)
        .all()
    )

    return [
        {
            "cnes": r.cnes,
            "estabelecimento": r.estabelecimento
        }
        for r in results
    ]


def get_procedimentos_list(db: Session):
    results = (
        db.query(
            TabDimProcedimento.procedimento,
            TabDimProcedimento.descricao
        )
        .order_by(TabDimProcedimento.descricao)
        .all()
    )

    return [
        {
            "procedimento": r.procedimento,
            "descricao": r.descricao
        }
        for r in results
    ]


def get_calendario_list(db: Session):
    results = (
        db.query(
            TabDimCalendario.ano,
            TabDimCalendario.anomes,
            TabDimCalendario.mes_ano_label
        )
        .order_by(TabDimCalendario.anomes)
        .all()
    )

    return [
        {
            "ano": r.ano,
            "anomes": r.anomes,
            "label": r.mes_ano_label
        }
        for r in results
    ]