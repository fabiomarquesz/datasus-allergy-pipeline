# ==============================================================
# DATASUS Allergy Pipeline — Makefile
# ==============================================================

.PHONY: help setup up down restart logs ps clean fernet

help:
	@echo ""
	@echo "  DATASUS Allergy Pipeline — Comandos"
	@echo "  ─────────────────────────────────────────────"
	@echo "  make setup    → Copia .env.example para .env"
	@echo "  make fernet   → Gera uma Fernet Key para o .env"
	@echo "  make up       → Sobe todos os containers"
	@echo "  make down     → Para e remove os containers"
	@echo "  make restart  → Restart completo"
	@echo "  make logs     → Logs em tempo real"
	@echo "  make ps       → Status dos containers"
	@echo "  make clean    → Remove containers + volumes (apaga dados!)"
	@echo ""

setup:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✅ .env criado. Edite as senhas antes de subir!"; \
	else \
		echo "⚠️  .env já existe. Edite manualmente se necessário."; \
	fi

fernet:
	@python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

up:
	docker compose up -d
	@echo "⏳ Aguarde ~60s para o Airflow inicializar..."
	@echo "🌐 Acesse: http://localhost:8080"

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

ps:
	docker compose ps

clean:
	docker compose down -v
	@echo "🗑️  Volumes removidos. Todos os dados foram apagados."
