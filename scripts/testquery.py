from backend.queries import (
    list_fato_detalhado,
    get_analitico
)
from backend.database import SessionLocal

# ==============================
# TESTE
# ==============================

def run_tests():
    db = SessionLocal()

    try:
        # ==============================
        # TESTE 1: LISTAR (SEM FILTRO)
        # ==============================
        print("=== TESTE 1: LISTAR TODOS ===")
        dados = list_fato_detalhado(db)
        print(f"Total registros: {len(dados)}")
        print(dados[:5])

        # ==============================
        # TESTE 2: FILTRO POR PROCEDIMENTO
        # ==============================
        print("\n=== TESTE 2: FILTRO PROCEDIMENTO ===")
        dados_proc = list_fato_detalhado(
            db,
            procedimento="0101010028"
        )
        print(f"Total registros: {len(dados_proc)}")
        print(dados_proc[:5])

        # ==============================
        # TESTE 3: FILTRO POR CNES
        # ==============================
        print("\n=== TESTE 3: FILTRO CNES ===")
        dados_cnes = list_fato_detalhado(
            db,
            cnes="2561492"
        )
        print(f"Total registros: {len(dados_cnes)}")
        print(dados_cnes[:5])

        # ==============================
        # TESTE 4: FILTRO POR ANO
        # ==============================
        print("\n=== TESTE 4: FILTRO ANO ===")
        dados_ano = list_fato_detalhado(
            db,
            ano="2025"
        )
        print(f"Total registros: {len(dados_ano)}")
        print(dados_ano[:5])

        # ==============================
        # TESTE 5: AGREGAÇÃO
        # ==============================
        print("\n=== TESTE 5: AGREGAÇÃO ===")
        agg = get_analitico(db)
        print(f"Total registros: {len(agg)}")
        print(agg[:5])

        # ==============================
        # TESTE 6: AGREGAÇÃO FILTRADA
        # ==============================
        print("\n=== TESTE 6: AGREGAÇÃO FILTRADA ===")
        agg_filtro = get_analitico(
            db,
            cnes=["2561492"],
            ano=["2025"]
        )
        print(f"Total registros: {len(agg_filtro)}")
        print(agg_filtro[:5])

    finally:
        db.close()


# ==============================
# EXECUÇÃO
# ==============================

if __name__ == "__main__":
    run_tests()

# poetry run python -m scripts.testquery # Roda
# poetry run python scripts/testquery.py # Roda precisa configurar a importacao do backend, ou seja, precisa rodar a partir da raiz do projeto
