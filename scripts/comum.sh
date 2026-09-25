#!/bin/bash
# ---------------------------------------------------------------------
# Funções compartilhadas pelos scripts de demonstração.
# Carrega o .env para que os scripts usem a mesma configuração do compose.
# ---------------------------------------------------------------------
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$RAIZ"
set -a; source "$RAIZ/.env"; set +a

LOG_DIR="$RAIZ/logs"
mkdir -p "$LOG_DIR"

# Espera entre etapas das demonstrações (segundos); ESPERA=N na linha de comando sobrescreve o .env
ESPERA="${ESPERA:-$DEMO_ESPERA_SECONDS}"

#######################################
# Imprime um título destacado com horário.
# Argumentos: texto do título
#######################################
titulo() { printf '\n\033[1;36m==== [%s] %s ====\033[0m\n' "$(date +%H:%M:%S)" "$*"; }

#######################################
# Retorna o nome de um broker Kafka em execução (para rodar as CLIs).
#######################################
broker_vivo() {
  for b in ${KAFKA_BROKER_CONTAINERS//,/ }; do
    if [ "$(docker inspect -f '{{.State.Running}}' "$b" 2>/dev/null)" = "true" ]; then echo "$b"; return; fi
  done
  echo "nenhum broker em execução" >&2; return 1
}

#######################################
# Executa uma ferramenta de linha de comando do Kafka num broker vivo.
# Argumentos: nome do script (ex.: kafka-topics.sh) e seus parâmetros
#######################################
kafka_cli() {
  local tool="$1"; shift
  docker exec "$(broker_vivo)" "/opt/kafka/bin/$tool" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@" 2>/dev/null
}

#######################################
# Mostra líder, réplicas e ISR de cada partição do tópico principal.
#######################################
descrever_topico() { kafka_cli kafka-topics.sh --describe --topic "$TOPIC_NAME" | sed 's/\t/  /g'; }

#######################################
# Mostra quem consome cada partição e o lag do grupo.
#######################################
descrever_grupo() {
  kafka_cli kafka-consumer-groups.sh --describe --group "$CONSUMER_GROUP_ID" \
    | awk 'NF' | awk '{printf "%-24s %-15s %-10s %-15s %-15s %-6s %s\n", $1,$2,$3,$4,$5,$6,$7}'
}

#######################################
# Lista os membros do grupo e quantas partições cada um tem
# (inclui membros ociosos, com 0 partições).
#######################################
descrever_membros() {
  kafka_cli kafka-consumer-groups.sh --describe --group "$CONSUMER_GROUP_ID" --members \
    | awk 'NF' | awk '{printf "%-50s %-12s %s\n", $2,$4,$5}' | sed 's/^CONSUMER-ID/MEMBRO/'
}

#######################################
# Executa uma consulta SQL no PostgreSQL.
# Argumentos: comando SQL
#######################################
sql() { docker exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"; }

#######################################
# Conta as leituras gravadas nos últimos N segundos (prova que o sistema está vivo).
# Argumentos: janela em segundos (padrão 10)
#######################################
vazao() {
  local j="${1:-10}"
  docker exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT count(*) FROM leituras WHERE ts_processado > now() - interval '$j seconds'"
}

#######################################
# Lista os containers de processador em execução.
#######################################
processadores() { docker ps --filter "label=com.docker.compose.service=processador" --format '{{.Names}}' | sort; }

#######################################
# Redireciona toda a saída do script também para um arquivo em logs/.
# Argumentos: prefixo do nome do arquivo
#######################################
gravar_log() {
  local arq="$LOG_DIR/$1-$(date +%Y%m%d-%H%M%S).log"
  exec > >(tee "$arq") 2>&1
  echo "(saída gravada em ${arq#$RAIZ/})"
}
