#!/bin/bash
# ---------------------------------------------------------------------
# Teste de falha 2: derruba um processador (consumidor) e mostra que
# outro processador do grupo assume as partições dele (rebalanço).
#
# Usa "docker kill" (SIGKILL): o processo morre sem avisar o grupo,
# como numa queda real. O coordenador só percebe quando o consumidor
# deixa de mandar heartbeats por SESSION_TIMEOUT_MS; aí dispara o
# rebalanço e as partições órfãs vão para os sobreviventes, que
# continuam a partir do último offset commitado (nada se perde).
#
# Uso: scripts/falha-consumidor.sh [nome-do-container]
#      (padrão: o primeiro processador em execução)
# ---------------------------------------------------------------------
source "$(dirname "$0")/comum.sh"
gravar_log "falha-consumidor"

VITIMA="${1:-$(processadores | head -n1)}"
HOST_VITIMA="$(docker inspect -f '{{.Config.Hostname}}' "$VITIMA")"

titulo "ANTES: distribuição das partições entre os processadores"
descrever_grupo

titulo "DERRUBANDO $VITIMA (proc-${HOST_VITIMA:0:8}) com SIGKILL"
docker kill "$VITIMA" >/dev/null

ESPERA_REB=$(( SESSION_TIMEOUT_MS / 1000 + 10 ))
titulo "Aguardando ${ESPERA_REB}s (session.timeout=${SESSION_TIMEOUT_MS}ms + margem) para o rebalanço..."
sleep "$ESPERA_REB"

titulo "DEPOIS: as partições do processador morto foram redistribuídas"
descrever_grupo

titulo "Logs de REBALANÇO dos sobreviventes"
docker compose logs --since "$(( ESPERA_REB + 5 ))s" processador | grep -E "REBALANÇO" || echo "(nenhum)"

titulo "Registro na tabela eventos_rebalanco"
sql "SELECT to_char(ts,'HH24:MI:SS') hora, processador, evento, particoes
     FROM eventos_rebalanco WHERE ts > now() - interval '$(( ESPERA_REB + 30 )) seconds' ORDER BY ts DESC LIMIT 15"

titulo "Quem gravou leituras nos últimos 10s (o morto some)"
sql "SELECT processador, array_agg(DISTINCT kafka_partition ORDER BY kafka_partition) particoes, count(*) leituras
     FROM leituras WHERE ts_processado > now() - interval '10 seconds' GROUP BY 1 ORDER BY 1"

echo
echo "Para religar: docker start $VITIMA   (ou: make up)"
