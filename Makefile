# =====================================================================
# Fábrica Inteligente: Kafka com balanceamento, elasticidade e failover
# Rode "make help" para ver os comandos.
# =====================================================================

include .env
export

COMPOSE := docker compose
SH      := bash scripts
N       ?= 3
BROKER  ?= $(DEMO_BROKER)

.DEFAULT_GOAL := help
.PHONY: help build up down clean restart ps status logs logs-sensores logs-processadores \
        rebalancos topico grupo alertas db ui scale-processadores scale-sensores \
        falha-broker falha-consumidor falha-dois-brokers elasticidade demo parar-broker religar-broker

help: ## Lista os comandos disponíveis
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------- ciclo de vida ----------
build: ## Constrói as imagens do sensor e do processador
	$(COMPOSE) build

up: build ## Sobe todo o ambiente (brokers, tópicos, banco, sensores, processadores, UI)
	$(COMPOSE) up -d
	@echo "Kafka UI: http://localhost:$(KAFKA_UI_PORT)"

down: ## Para e remove os containers (mantém os dados)
	$(COMPOSE) down

clean: ## Remove containers, volumes (dados) e logs gerados
	$(COMPOSE) down -v --remove-orphans
	rm -f logs/*.log

restart: down up ## Reinicia tudo

# ---------- observação ----------
ps: ## Lista os containers
	$(COMPOSE) ps

status: ## Painel: containers, partições/ISR, consumer group, banco
	@$(SH)/status.sh

topico: ## Descreve o tópico (líder, réplicas e ISR de cada partição)
	@bash -c 'source scripts/comum.sh; descrever_topico'

grupo: ## Mostra qual processador lê cada partição e o lag
	@bash -c 'source scripts/comum.sh; descrever_grupo'

logs: ## Acompanha os logs de todos os serviços
	$(COMPOSE) logs -f --tail=20

logs-sensores: ## Acompanha os logs dos sensores
	$(COMPOSE) logs -f --tail=20 sensor

logs-processadores: ## Acompanha os logs dos processadores
	$(COMPOSE) logs -f --tail=20 processador

rebalancos: ## Mostra os rebalanços nos logs dos processadores
	$(COMPOSE) logs --no-color processador | grep REBALANÇO

alertas: ## Últimos alertas gravados no banco
	@bash -c 'source scripts/comum.sh; sql "SELECT to_char(ts_detectado,'"'"'HH24:MI:SS'"'"') hora, maquina, tipo, valor, limite, processador FROM alertas ORDER BY id DESC LIMIT 20"'

db: ## Abre um psql no banco
	docker exec -it postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

ui: ## Abre o Kafka UI no navegador
	open http://localhost:$(KAFKA_UI_PORT) || xdg-open http://localhost:$(KAFKA_UI_PORT)

# ---------- elasticidade ----------
scale-processadores: ## Escala os processadores (make scale-processadores N=5)
	$(COMPOSE) up -d --no-recreate --scale processador=$(N) processador

scale-sensores: ## Escala os sensores (make scale-sensores N=6)
	$(COMPOSE) up -d --no-recreate --scale sensor=$(N) sensor

# ---------- falhas (manuais) ----------
parar-broker: ## Derruba um broker (make parar-broker BROKER=kafka1)
	docker kill $(BROKER)

religar-broker: ## Religa um broker (make religar-broker BROKER=kafka1)
	docker start $(BROKER)

# ---------- demonstrações roteirizadas (gravam em logs/) ----------
falha-broker: ## Demo: derruba e religa um broker (BROKER=kafka2)
	$(SH)/falha-broker.sh $(BROKER)

falha-consumidor: ## Demo: mata um processador e mostra o rebalanço
	$(SH)/falha-consumidor.sh

falha-dois-brokers: ## Demo: derruba 2 brokers (limite do sistema) e religa
	$(SH)/falha-dois-brokers.sh

elasticidade: ## Demo: aumenta carga e escala processadores
	$(SH)/elasticidade.sh

demo: ## Roda as três demonstrações em sequência
	$(SH)/falha-broker.sh $(BROKER)
	$(COMPOSE) up -d --scale processador=$(CONSUMER_REPLICAS) processador
	$(SH)/falha-consumidor.sh
	$(COMPOSE) up -d --scale processador=$(CONSUMER_REPLICAS) processador
	$(SH)/elasticidade.sh
