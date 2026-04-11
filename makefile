# ==============================
# CONFIG
# ==============================

.PHONY: run run-dev run-prod run-backend run-frontend run-pipeline run-all clean test

# ==============================
# BACKEND
# ==============================

run-backend:
	@echo "🚀 Subindo backend..."
	poetry run uvicorn backend.main:app --reload

# ==============================
# FRONTEND
# ==============================

run-frontend:
	@echo "🎨 Subindo frontend..."
	poetry run streamlit run frontend/app.py

# ==============================
# PIPELINE
# ==============================

run-pipeline:
	@echo "🔄 Executando pipeline..."
	poetry run python rodar_pipeline.py

# ==============================
# EXECUÇÃO COMPLETA (DEV)
# ==============================

run-dev:
	@echo "🚀 Subindo backend + frontend (DEV)..."
	poetry run uvicorn backend.main:app --reload &
	BACK_PID=$$!
	sleep 2
	poetry run streamlit run frontend/app.py
	kill $$BACK_PID

# ==============================
# EXECUÇÃO COMPLETA (PROD SIMPLES)
# ==============================

run-prod:
	@echo "🚀 Subindo backend + frontend (PROD)..."
	poetry run uvicorn backend.main:app &
	BACK_PID=$$!
	sleep 2
	poetry run streamlit run frontend/app.py
	kill $$BACK_PID

# ==============================
# PIPELINE + SISTEMA
# ==============================

run-all:
	@echo "🔄 Rodando pipeline completo..."
	poetry run python rodar_pipeline.py
	@echo "🚀 Subindo aplicação..."
	$(MAKE) run-dev

# ==============================
# LIMPEZA (DADOS LOCAIS)
# ==============================

clean:
	@echo "🧹 Limpando dados..."
	rm -rf data/bronze/*
	rm -rf data/silver/*
	rm -rf data/silver_cnes/*
	rm -rf data/gold/*
	rm -rf data/db/*.db
	@echo "✅ Limpeza concluída"

# ==============================
# TESTES
# ==============================

test:
	@echo "🧪 Rodando testes..."
	poetry run pytest -s tests/

# ==============================
# TESTES DEV (VALIDAÇÃO RÁPIDA)
# ==============================

test-dev:
	@echo "🧪 Rodando testes DEV (qualidade de dados)..."
	poetry run pytest -s \
	tests/test_completude_mensal.py \
	tests/test_quality_number_anual.py \
	tests/test_variacao_mensal.py


# ==============================
# GIT
# ==============================

git-status:
	@echo "📌 Status do Git"
	git status

git-add:
	@echo "➕ Adicionando arquivos"
	git add .

git-commit:
	@echo "📝 Commitando alterações"
	git commit -m "update: nova versão do projeto"

git-push:
	@echo "🚀 Enviando para o GitHub"
	git push origin main

git-all:
	@echo "🚀 Subindo versão completa..."
	git status
	git add .
	git commit -m "$(msg)"
	git push origin main

#feat: nova funcionalidade
#fix: correção
#test: testes
#refactor: refatoração
#chore: manutenção
# make git-all msg="fix: nova versão da dimensão calendário, novos testes e ajustes no pipeline e correçao dos testes"

# ==============================
# AJUDA
# ==============================

help:
	@echo ""
	@echo "📌 Comandos disponíveis:"
	@echo ""
	@echo "make run-backend   → roda apenas backend"
	@echo "make run-frontend  → roda apenas frontend"
	@echo "make run-dev       → roda backend + frontend (dev)"
	@echo "make run-prod      → roda backend + frontend (prod simples)"
	@echo "make run-pipeline  → executa pipeline"
	@echo "make run-all       → pipeline + sistema"
	@echo "make clean         → limpa dados"
	@echo "make test          → roda testes"
	@echo "make test-dev      → roda testes DEV"
	@echo ""