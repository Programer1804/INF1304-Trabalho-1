#!/bin/bash
# ---------------------------------------------------------------------
# Demonstração de elasticidade.
#
# Etapa 1: aumenta a carga (mais sensores) com processamento "lento",
#          fazendo o lag do grupo crescer.
# Etapa 2: escala os processadores até o nº de partições; cada novo
#          processador dispara um rebalanço e o lag volta a cair.
# Etapa 3: escala além do nº de partições para mostrar o limite:
#          processadores extras ficam ociosos (sem partição).
# Etapa 4: volta ao tamanho original (scale down também rebalanceia).
#
# Uso: scripts/elasticidade.sh
# ---------------------------------------------------------------------
source "$(dirname "$0")/comum.sh"
gravar_log "elasticidade"

# Sobrescreve só nesta demo: processamento lento para gerar backlog visível
export PROCESSING_DELAY_MS="$DEMO_PROCESSING_DELAY_MS"
SENSORES_CARGA="$DEMO_SENSORES"

lag_total() { descrever_grupo | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}'; }

titulo "Estado inicial: $CONSUMER_REPLICAS processadores, $SENSOR_REPLICAS sensores"
descrever_grupo

titulo "ETAPA 1: carga sobe para $SENSORES_CARGA sensores, 1 processador lento (${PROCESSING_DELAY_MS}ms/msg)"
docker compose up -d --no-recreate --scale sensor="$SENSORES_CARGA" sensor >/dev/null 2>&1
docker compose up -d --force-recreate --scale processador=1 processador >/dev/null 2>&1
for i in 1 2 3; do sleep "$ESPERA"; echo "lag total do grupo: $(lag_total)"; done
descrever_grupo

for N in 3 "$TOPIC_PARTITIONS"; do
  titulo "ETAPA 2: escalando processadores para $N"
  docker compose up -d --no-recreate --scale processador="$N" processador >/dev/null 2>&1
  for i in 1 2 3; do sleep "$ESPERA"; echo "lag total do grupo: $(lag_total)"; done
  descrever_grupo
done

EXTRA=$(( TOPIC_PARTITIONS + 2 ))
titulo "ETAPA 3: $EXTRA processadores para $TOPIC_PARTITIONS partições (2 ficarão ociosos)"
docker compose up -d --no-recreate --scale processador="$EXTRA" processador >/dev/null 2>&1
sleep "$ESPERA"
descrever_membros
echo "(membros com #PARTITIONS = 0 estão no grupo mas ociosos: não há partição sobrando)"

titulo "Rebalanços registrados durante a demonstração"
sql "SELECT to_char(ts,'HH24:MI:SS') hora, processador, evento, particoes
     FROM eventos_rebalanco WHERE ts > now() - interval '15 minutes' ORDER BY ts DESC LIMIT 25"

titulo "ETAPA 4: voltando ao tamanho original (processamento normal)"
export PROCESSING_DELAY_MS=0
docker compose up -d --force-recreate --scale processador="$CONSUMER_REPLICAS" processador >/dev/null 2>&1
docker compose up -d --no-recreate --scale sensor="$SENSOR_REPLICAS" sensor >/dev/null 2>&1
sleep "$ESPERA"
descrever_grupo
echo "lag total do grupo: $(lag_total)"
