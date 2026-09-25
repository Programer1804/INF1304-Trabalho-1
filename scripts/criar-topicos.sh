#!/bin/bash
# ---------------------------------------------------------------------
# Cria os tópicos do sistema no cluster Kafka.
# Executado pelo serviço "kafka-init" do docker-compose; todos os
# parâmetros chegam por variáveis de ambiente (vindas do .env).
#
#   TOPIC_NAME        tópico de leituras dos sensores (dados-sensores)
#   ALERT_TOPIC_NAME  tópico onde os processadores publicam alertas
#   PARTITIONS        número de partições (unidade de paralelismo)
#   RF                fator de replicação (cópias de cada partição)
#   MIN_ISR           mínimo de réplicas sincronizadas p/ aceitar escrita
# ---------------------------------------------------------------------
set -euo pipefail

KT=/opt/kafka/bin/kafka-topics.sh

for T in "$TOPIC_NAME" "$ALERT_TOPIC_NAME"; do
  echo ">> criando tópico '$T' (partições=$PARTITIONS, RF=$RF, min.insync=$MIN_ISR)"
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$T" --partitions "$PARTITIONS" --replication-factor "$RF" \
      --config min.insync.replicas="$MIN_ISR"
done

echo ">> descrição do tópico principal:"
$KT --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC_NAME"
