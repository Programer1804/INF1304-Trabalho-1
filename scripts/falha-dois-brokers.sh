#!/bin/bash
# ---------------------------------------------------------------------
# Teste de falha 3 (limite do sistema): derruba DOIS brokers ao mesmo tempo.
#
# Com 3 nós, isso quebra duas garantias:
#   - o quórum KRaft (precisa de 2 de 3 controllers) é perdido;
#   - o ISR cai para 1 réplica < min.insync.replicas=2.
# Espera-se que o sistema PARE de aceitar escritas (proteção contra perda
# de dados). Os sensores seguram as mensagens no buffer e tentam de novo;
# quando os brokers voltam, o processamento é retomado.
#
# Uso: scripts/falha-dois-brokers.sh [brokerA] [brokerB]  (padrão: DEMO_BROKERS_DUPLA do .env)
# ---------------------------------------------------------------------
source "$(dirname "$0")/comum.sh"
A="${1:-${DEMO_BROKERS_DUPLA%%,*}}"; B="${2:-${DEMO_BROKERS_DUPLA#*,}}"
gravar_log "falha-dois-brokers"

titulo "ANTES"
echo "leituras gravadas nos últimos 10s: $(vazao 10)"

titulo "DERRUBANDO $A e $B"
docker kill "$A" "$B" >/dev/null
for i in 1 2 3; do
  sleep "$ESPERA"
  echo "[$(date +%H:%M:%S)] leituras gravadas nos últimos 10s: $(vazao 10)"
done

titulo "Sensores durante a falha (pendentes = mensagens retidas no buffer)"
docker compose logs --no-log-prefix --since "$(( ESPERA * 2 ))s" sensor | grep -E "enviadas=" | tail -n 3

titulo "RELIGANDO $A e $B"
docker start "$A" "$B" >/dev/null
for i in 1 2 3; do
  sleep "$ESPERA"
  echo "[$(date +%H:%M:%S)] leituras gravadas nos últimos 10s: $(vazao 10)"
done

titulo "DEPOIS: tópico e grupo"
descrever_topico
descrever_grupo
titulo "Sensores após a recuperação"
docker compose logs --no-log-prefix --since "${ESPERA}s" sensor | grep -E "enviadas=" | tail -n 3
