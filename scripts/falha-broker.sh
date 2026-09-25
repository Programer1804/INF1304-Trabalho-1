#!/bin/bash
# ---------------------------------------------------------------------
# Teste de falha 1: derruba um broker Kafka e mostra que o sistema
# continua funcionando.
#
# O que se espera ver:
#   - partições cujo líder era o broker derrubado elegem novo líder;
#   - o ISR (réplicas em sincronia) encolhe de 3 para 2;
#   - como min.insync.replicas=2, produtores com acks=all continuam escrevendo;
#   - a vazão de leituras gravadas no banco não vai a zero;
#   - ao religar o broker, ele volta ao ISR.
#
# Uso: scripts/falha-broker.sh [kafka1|kafka2|kafka3]   (padrão: DEMO_BROKER do .env)
# ---------------------------------------------------------------------
source "$(dirname "$0")/comum.sh"
BROKER="${1:-$DEMO_BROKER}"
gravar_log "falha-broker-$BROKER"

titulo "ANTES: estado do tópico (todos os brokers no ar)"
descrever_topico
echo "leituras gravadas nos últimos 10s: $(vazao 10)"

titulo "DERRUBANDO o broker $BROKER (docker kill = queda abrupta)"
docker kill "$BROKER" >/dev/null
echo "$BROKER parado."

titulo "Aguardando ${ESPERA}s para eleição de novos líderes..."
sleep "$ESPERA"

titulo "DURANTE a falha: líderes e ISR"
descrever_topico
echo
echo "leituras gravadas nos últimos 10s (sistema segue funcionando): $(vazao 10)"
titulo "DURANTE a falha: consumer group"
descrever_grupo

titulo "Últimas linhas dos sensores (sem erros de entrega esperados)"
docker compose logs --no-log-prefix --since "${ESPERA}s" sensor | tail -n 6

titulo "RELIGANDO o broker $BROKER"
docker start "$BROKER" >/dev/null
sleep "$ESPERA"

titulo "DEPOIS: broker de volta (volta ao ISR; lideranças podem ser rebalanceadas)"
descrever_topico
echo "leituras gravadas nos últimos 10s: $(vazao 10)"
