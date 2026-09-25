# Fábrica Inteligente: Kafka com Balanceamento, Elasticidade e Failover

Trabalho 1 de Distribuição e Concorrência (2026/1).

Sensores simulados publicam leituras (temperatura, vibração, consumo) num cluster
Kafka de 3 brokers. Processadores em Python, todos no mesmo consumer group,
dividem as partições entre si, detectam anomalias e gravam leituras e alertas
no PostgreSQL. A arquitetura e os resultados estão em [RELATORIO.md](RELATORIO.md).

## 1. Pré-requisitos

| Ferramenta | Versão testada | Observação |
|---|---|---|
| Docker Engine | 29.x | No macOS usamos **Colima** (`colima start --cpu 4 --memory 6`) ou Docker Desktop |
| Docker Compose | v2+ (plugin `docker compose`) | |
| GNU Make | 3.81+ | |
| RAM livre para o Docker | ≥ 4 GB (recomendado 6 GB) | 3 brokers Kafka + Postgres + UI |

Não é preciso ter Python nem Kafka instalados na máquina: tudo roda em containers.

## 2. Instalação

```bash
cd "INF1304-Trabalho 1"   # pasta do projeto
make up              # constrói as imagens e sobe todo o ambiente
```

`make up` faz, em ordem:
1. constrói as imagens `fabrica/sensor` e `fabrica/processador`;
2. sobe `kafka1`, `kafka2` e `kafka3` e espera o healthcheck;
3. roda `kafka-init`, que cria os tópicos `dados-sensores` e `alertas`
   (6 partições, RF=3, `min.insync.replicas=2`);
4. sobe o PostgreSQL (o schema vem de `db/init.sql`);
5. sobe 3 sensores, 3 processadores e o Kafka UI.

Painel web: **http://localhost:8080** (Kafka UI: brokers, partições, consumer group, lag).

## 3. Configuração

Todos os parâmetros ficam no arquivo **`.env`**, sem constantes no código. O
`docker-compose.yml` injeta essas variáveis como variáveis de ambiente nos containers.
Principais:

| Variável | Padrão | Efeito |
|---|---|---|
| `TOPIC_PARTITIONS` | 6 | nº de partições = paralelismo máximo do grupo |
| `TOPIC_REPLICATION_FACTOR` | 3 | cópias de cada partição |
| `TOPIC_MIN_INSYNC_REPLICAS` | 2 | réplicas que precisam confirmar cada escrita |
| `SENSOR_REPLICAS` / `CONSUMER_REPLICAS` | 3 / 3 | tamanho inicial de cada serviço |
| `MACHINES_PER_SENSOR` | 8 | máquinas simuladas por container de sensor |
| `SENSOR_INTERVAL_SECONDS` | 1.0 | período entre leituras |
| `SENSOR_ANOMALY_PROBABILITY` | 0.05 | chance de uma leitura anômala |
| `LIMIT_TEMPERATURE_C`, `LIMIT_VIBRATION_MM_S`, `LIMIT_POWER_KW` | 85 / 7.1 / 45 | limites de alerta |
| `PARTITION_ASSIGNMENT_STRATEGY` | range | `range`, `roundrobin` ou `cooperative-sticky` |
| `SESSION_TIMEOUT_MS` | 10000 | tempo até um consumidor morto sair do grupo |
| `PROCESSING_DELAY_MS` | 0 | atraso artificial por mensagem (simula carga) |
| `DEMO_BROKER`, `DEMO_BROKERS_DUPLA` | kafka2 / kafka1,kafka2 | brokers derrubados pelas demos de falha |
| `DEMO_ESPERA_SECONDS` | 20 | pausa entre as etapas das demos |
| `DEMO_SENSORES`, `DEMO_PROCESSING_DELAY_MS` | 6 / 40 | carga e lentidão usadas na demo de elasticidade |

Depois de mudar o `.env`, rode `make up` de novo.

## 4. Operação

Rode `make help` para ver todos os comandos.

### Observar

| Comando | O que mostra |
|---|---|
| `make status` | containers, líder/ISR de cada partição, qual processador lê cada partição, contagens no banco |
| `make topico` | líder, réplicas e ISR por partição |
| `make grupo` | partição → processador, offsets e lag |
| `make logs-sensores` / `make logs-processadores` | logs ao vivo |
| `make rebalancos` | só as linhas de rebalanço dos processadores |
| `make alertas` | últimos 20 alertas gravados |
| `make db` | abre um `psql` (tabelas `leituras`, `alertas`, `eventos_rebalanco`) |
| `make ui` | abre o Kafka UI |

### Elasticidade (manual)

```bash
make scale-sensores N=6          # mais carga
make scale-processadores N=6     # mais consumidores -> rebalanço automático
make scale-processadores N=2     # scale down -> rebalanço automático
```

### Falhas (manual)

```bash
make parar-broker BROKER=kafka1      # queda abrupta de um broker
make topico                          # novo líder eleito, ISR com 2 réplicas
make religar-broker BROKER=kafka1

docker kill fabrica-processador-1    # queda abrupta de um consumidor
make rebalancos                      # sobreviventes assumem as partições
docker start fabrica-processador-1
```

### Demonstrações roteirizadas (gravam saída em `logs/`)

| Comando | Script | Duração |
|---|---|---|
| `make falha-broker BROKER=kafka2` | `scripts/falha-broker.sh` | ~1 min |
| `make falha-consumidor` | `scripts/falha-consumidor.sh` | ~30 s |
| `make falha-dois-brokers` | `scripts/falha-dois-brokers.sh` | ~2 min |
| `make elasticidade` | `scripts/elasticidade.sh` | ~4 min |
| `make demo` | as três em sequência | ~6 min |

A variável `ESPERA` sobrescreve a pausa entre as etapas (padrão: `DEMO_ESPERA_SECONDS` do `.env`):
`ESPERA=10 make falha-broker`.

**No macOS, rode as demos com `caffeinate`** para o computador não entrar em repouso no meio:

```bash
caffeinate -dims make demo
```

### Desligar

```bash
make down     # para tudo e mantém os dados
make clean    # para tudo e apaga volumes (Kafka e Postgres) e logs
```

## 5. Estrutura

```
.
├── .env                      # toda a configuração
├── docker-compose.yml        # todos os serviços (YAML)
├── Makefile
├── sensor/                   # produtor (Python + confluent-kafka)
│   ├── sensor.py
│   ├── requirements.txt
│   └── Dockerfile
├── processador/              # consumidor (Python + confluent-kafka + psycopg2)
│   ├── processador.py
│   ├── requirements.txt
│   └── Dockerfile
├── db/init.sql               # schema do PostgreSQL
├── scripts/
│   ├── criar-topicos.sh      # usado pelo serviço kafka-init
│   ├── comum.sh              # funções compartilhadas
│   ├── status.sh
│   ├── falha-broker.sh
│   ├── falha-consumidor.sh
│   ├── falha-dois-brokers.sh
│   └── elasticidade.sh
├── logs/                     # saídas das demonstrações e logs de rebalanço
└── RELATORIO.md
```

## 6. Problemas comuns

- **`Cannot connect to the Docker daemon`**: o Docker não está rodando. No macOS com Colima, use `colima start --cpu 4 --memory 6`.
- **Brokers reiniciando / `OOMKilled`**: pouca memória para a VM do Docker. Aumente para 6 GB.
- **Porta 8080 ocupada**: mude `KAFKA_UI_PORT` no `.env`.
- **Uma demo "trava" e as esperas duram minutos**: o Mac entrou em repouso e pausou a VM do Docker. As consultas por janela de tempo ("últimos 10 s") saem vazias. Rode de novo com `caffeinate -dims make <demo>`.
- **Mensagens `Failed to resolve 'kafka1:9092'` nos logs** durante o teste de broker: é o esperado. O cliente avisa que um broker sumiu e continua usando os outros.
