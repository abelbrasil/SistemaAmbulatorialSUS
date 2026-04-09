from fastapi import FastAPI
from backend.router import router

app = FastAPI(
    title="API Produção SIASUS",
    version="1.0.0",
    description="API para consulta de produção ambulatorial"
)

# incluir rotas
app.include_router(router)


@app.get("/")
def root():
    return {"status": "API online"}

# poetry run uvicorn backend.main:app --reload