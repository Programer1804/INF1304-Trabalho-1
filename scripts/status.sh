#!/bin/bash
# ---------------------------------------------------------------------
# Painel rápido do sistema: containers, tópico, grupo e banco.
# Uso: scripts/status.sh
# ---------------------------------------------------------------------
source "$(dirname "$0")/comum.sh"

titulo "Containers"
docker compose ps --format 'table {{.Name}}\t{{.State}}\t{{.Status}}'

titulo "Tópico $TOPIC_NAME (líder / réplicas / ISR por partição)"
descrever_topico

titulo "Consumer group $CONSUMER_GROUP_ID (partição -> processador, lag)"
descrever_grupo

titulo "Banco de dados"
sql "SELECT (SELECT count(*) FROM leituras) leituras,
            (SELECT count(*) FROM alertas) alertas,
            (SELECT count(*) FROM eventos_rebalanco) rebalancos"
sql "SELECT processador, count(*) leituras_ultimos_30s
     FROM leituras WHERE ts_processado > now() - interval '30 seconds' GROUP BY 1 ORDER BY 1"
