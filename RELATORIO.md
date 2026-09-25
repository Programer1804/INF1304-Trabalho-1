# Relatório: Balanceamento de Carga, Elasticidade e Failover com Kafka

**Disciplina:** Distribuição e Concorrência, 2026/1 · **Trabalho 1**
**Mini-mundo:** Sistema de Monitoramento de Sensores em uma Fábrica Inteligente
**Grupo:** Matheus Figueiredo, João Pedro Feijoo e João Marcelo Onofre

Instalação e operação estão no [README.md](README.md). Este relatório cobre a
arquitetura, os testes de falha, os resultados e o que funcionou e não funcionou.

---

## 1. Arquitetura

```
                       rede docker "fabrica"
 ┌──────────────┐                                          ┌────────────────────┐
 │ sensor (×N)  │  JSON, chave = máquina                   │ processador (×M)   │
 │ 8 máquinas   │──────────┐                    ┌─────────▶│ group.id =         │
 │ por container│          ▼                    │          │ processadores-     │
 └──────────────┘  ┌────────────────────────────┴───┐      │ sensores           │
 ┌──────────────┐  │  Cluster Kafka (KRaft, 3 nós)   │      │ - detecta anomalia │
 │ sensor       │─▶│  kafka1   kafka2   kafka3       │─────▶│ - grava no banco   │
 └──────────────┘  │  tópico dados-sensores          │      │ - publica alertas  │
 ┌──────────────┐  │  6 partições × RF 3             │◀─────│   (tópico alertas) │
 │ sensor       │─▶│  min.insync.replicas = 2        │      └─────────┬──────────┘
 └──────────────┘  └────────────────────────────────┘                │
                               ▲                                     ▼
                        ┌──────┴──────┐                     ┌────────────────┐
                        │  kafka-ui   │ :8080               │  PostgreSQL    │
                        └─────────────┘                     │ leituras       │
                                                            │ alertas        │
                                                            │ eventos_rebal. │
                                                            └────────────────┘
```

### 1.1 Componentes

| Componente | Implementação | Requisito do enunciado |
|---|---|---|
| **Sensores (produtores)** | `sensor/sensor.py`, Python 3.12 + `confluent-kafka`. Cada container simula 8 máquinas de um setor (linha de produção, refrigeração ou empacotamento) e publica 1 leitura/s por máquina com temperatura, vibração e consumo | produtores em containers distintos, tópico `dados-sensores` |
| **Cluster Kafka** | 3 containers `apache/kafka:3.9.1` em modo **KRaft** (cada nó é broker e controller, sem Zookeeper) | 2 ou mais brokers em containers diferentes |
| **Tópico** | `dados-sensores`: 6 partições, fator de replicação 3, `min.insync.replicas=2` (criado pelo serviço `kafka-init`) | partições + replicação |
| **Processadores (consumidores)** | `processador/processador.py`, Python + `confluent-kafka` + `psycopg2`, todos no grupo `processadores-sensores` | consumidores no mesmo grupo com balanceamento automático |
| **Banco** | PostgreSQL 16, tabelas `leituras`, `alertas` e `eventos_rebalanco` | dados processados e alertas salvos |
| **Observabilidade** | logs estruturados, tabela `eventos_rebalanco`, Kafka UI, scripts `status`/`grupo`/`topico` | demonstração visual e via logs |

### 1.2 Decisões de projeto

- **Chave = ID da máquina.** O particionador faz `hash(chave) mod 6`, então todas as
  leituras de uma máquina vão, em ordem, para a mesma partição. As máquinas se espalham
  pelas 6 partições. Na primeira versão cada sensor tinha só 4 máquinas (12 chaves) e a
  partição 3 ficou vazia. Com 8 máquinas (24 chaves) todas as partições recebem dados.
- **Durabilidade na produção:** `acks=all` + `enable.idempotence=true`. A escrita só é
  confirmada quando todas as réplicas do ISR gravaram, e reenvios não geram duplicatas.
- **RF=3 com `min.insync.replicas=2`:** tolera a queda de 1 broker sem parar as escritas
  e sem risco de perder mensagens já confirmadas.
- **Quórum KRaft de 3 controllers:** os metadados continuam disponíveis com 1 nó fora (maioria 2/3).
  Os tópicos internos (`__consumer_offsets`) também têm RF=3, então o consumer group
  sobrevive à queda do broker que era o seu coordenador.
- **Commit manual de offset (at-least-once):** `enable.auto.commit=false`. O processador
  grava o lote no Postgres numa transação e **só depois** faz commit do offset. No
  `on_revoke` (antes de perder uma partição) ele faz um commit síncrono, para que o
  próximo dono continue exatamente de onde parou.
- **Idempotência no banco:** `UNIQUE (kafka_partition, kafka_offset)` + `ON CONFLICT DO NOTHING`.
  Se uma mensagem for reprocessada depois de um rebalanço, ela não é duplicada.
- **Callbacks de rebalanço** (`on_assign`, `on_revoke`, `on_lost`) registram cada mudança
  no log (`>>> REBALANÇO: RECEBIDAS…`, `<<< REBALANÇO: REVOGADAS…`) e na tabela
  `eventos_rebalanco`. É isso que usamos como evidência nos testes.
- **Sem constantes hard-coded:** todos os parâmetros ficam no `.env`, são injetados pelo
  `docker-compose.yml` como variáveis de ambiente e lidos pela função `env()` em cada programa.
  O código está documentado com DocStrings.
- **Elasticidade:** sensores e processadores não têm `container_name` fixo, então
  `docker compose up --scale processador=N` cria e remove instâncias livremente. Cada
  instância usa o hostname do container como ID.

### 1.3 Formato da mensagem

```json
{
  "sensor_id": "sensor-2bf40fef",
  "maquina": "refrigeracao-2bf40fef-m3",
  "setor": "refrigeracao",
  "timestamp": "2026-09-24T21:29:26.329+00:00",
  "temperatura_c": 61.42,
  "vibracao_mm_s": 3.08,
  "consumo_kw": 24.77
}
```

Limites de alerta (`.env`): temperatura > 85 °C, vibração > 7,1 mm/s (faixa "insatisfatória"
da ISO 10816) e consumo > 45 kW. Cerca de 5% das leituras são anômalas de propósito
(`SENSOR_ANOMALY_PROBABILITY=0.05`).

---

## 2. Testes de falha e resultados

Todos os testes foram executados com os scripts em `scripts/` e a saída completa está em `logs/`.
Ambiente: macOS, Colima (4 vCPU, 6 GB), Docker 29, 3 sensores × 8 máquinas ≈ 24 msg/s.
Os logs são da execução de 25/09/2026, rodada com `caffeinate` para o Mac não entrar em repouso
(o repouso pausa a VM do Docker e distorce as esperas dos scripts).

### 2.1 Queda de um broker (`make falha-broker BROKER=kafka1`)

Log: `logs/falha-broker-kafka1-*.log`

| Partição | Líder antes | ISR antes | Líder durante a falha | ISR durante | Depois de religar |
|---|---|---|---|---|---|
| 0 | 3 | 3,1,2 | 3 | 3,2 | ISR 3,2,1 |
| **1** | **1** | 1,2,3 | **2** | 2,3 | ISR 2,3,1 |
| 2 | 2 | 2,3,1 | 2 | 2,3 | ISR 2,3,1 |
| **3** | **1** | 1,2,3 | **2** | 2,3 | ISR 2,3,1 |
| 4 | 2 | 2,3,1 | 2 | 2,3 | ISR 2,3,1 |
| 5 | 3 | 3,1,2 | 3 | 3,2 | ISR 3,2,1 |

- As partições lideradas pelo broker 1 (partições 1 e 3) **elegeram novo líder** automaticamente.
- Vazão gravada no banco (leituras/10 s): **240 antes → 240 durante → 240 depois**.
- Sensores: `falhas=0` durante todo o teste. O cliente só registra que `kafka1` ficou
  inalcançável e segue usando os outros brokers.
- O consumer group não foi afetado: mesma distribuição de partições, lag ≈ 0.
- Ao religar, o broker 1 volta a todos os ISRs. A liderança não volta para ele na hora
  (ver §3).

**Conclusão:** ✅ o sistema continua funcionando com um broker fora.

### 2.2 Queda de um consumidor (`make falha-consumidor`)

Log: `logs/falha-consumidor-*.log`

O script mata um processador com `docker kill` (SIGKILL, sem saída limpa do grupo).

```
ANTES                                   DEPOIS (11 s)
p0,p1 -> proc-573d9f78                  p0,p1,p2 -> proc-573d9f78
p2,p3 -> proc-711d8e46  (morto)         p3,p4,p5 -> proc-7b14f567
p4,p5 -> proc-7b14f567
```

Trecho do log dos sobreviventes:
```
11:18:13 [proc-7b14f567] <<< REBALANÇO: REVOGADAS partições [4, 5] | restam []
11:18:13 [proc-7b14f567] >>> REBALANÇO: RECEBIDAS partições [3, 4, 5] | agora responsável por [3, 4, 5]
11:18:13 [proc-573d9f78] <<< REBALANÇO: REVOGADAS partições [0, 1] | restam []
11:18:13 [proc-573d9f78] >>> REBALANÇO: RECEBIDAS partições [0, 1, 2] | agora responsável por [0, 1, 2]
```

- O rebalanço acontece 11 s depois da queda (kill às 11:18:02, rebalanço às 11:18:13), que é o `SESSION_TIMEOUT_MS`: o coordenador
  só declara o membro morto depois de 10 s sem heartbeat.
- As partições órfãs continuam **a partir do último offset commitado**, sem perder mensagens.
- Quando o processador é religado (`docker start`), um novo rebalanço devolve 2 partições para
  cada um (evento de 11:17:37 em `logs/eventos-rebalanco.csv`: `proc-711d8e46` recebe `{2,3}`).
- Com a estratégia `range` o rebalanço é *eager*: todos os membros devolvem todas as
  partições e recebem uma nova distribuição.

**Conclusão:** ✅ outro processador assume as partições do que caiu.

### 2.3 Elasticidade (`make elasticidade`)

Log: `logs/elasticidade-*.log`. Nesta demo o processamento foi deixado lento de propósito
(`PROCESSING_DELAY_MS=40`, capacidade ≈ 25 msg/s por processador) e a carga dobrou para
6 sensores (≈ 48 msg/s).

| Etapa | Sensores | Processadores | Lag total do grupo (amostras a cada 20 s) |
|---|---|---|---|
| 1. Carga alta, 1 processador | 6 | 1 | 752 → 1160 → **1668** (crescendo) |
| 2a. Scale-up | 6 | 3 | 1492 → 1070 → 821 (drenando) |
| 2b. Scale-up | 6 | 6 (1 por partição) | 328 → 68 → 80 (estável) |
| 3. Além do limite | 6 | 8 | 6 membros com 1 partição, **2 ociosos** (0 partições) |
| 4. Volta ao normal | 3 | 3 | 0 |

- Cada `--scale` gerou um rebalanço automático, registrado em `eventos_rebalanco`
  (histórico completo em `logs/eventos-rebalanco.csv`).
- A etapa 3 mostra que **o paralelismo do grupo é limitado pelo número de partições**.

**Conclusão:** ✅ a capacidade de processamento cresce com o número de consumidores até o nº de partições.

### 2.4 Teste extra: queda de dois brokers (`scripts/falha-dois-brokers.sh`)

Log: `logs/falha-dois-brokers-*.log`. Serve para mostrar o limite da configuração.

| Momento | Leituras gravadas / 10 s | Sensores |
|---|---|---|
| Antes | 240 | normal |
| 20 s, 40 s e 60 s com kafka1 e kafka2 fora | **0, 0, 0** | `enviadas` parado, `pendentes` 360 → 440, `falhas=0` |
| 20 s, 40 s e 60 s depois de religar | 0, 240, 240 | fila esvaziada (`pendentes=8`), envio normal |

Com 2 dos 3 nós fora, o quórum KRaft e o `min.insync.replicas=2` deixam de ser atendidos.
O cluster **para de aceitar escritas em vez de arriscar perder dados**. Os sensores guardam as
mensagens no buffer local e as entregam quando o cluster volta. A primeira amostra depois de
religar ainda é 0 porque os brokers levam alguns segundos para subir, refazer o quórum e eleger líderes.

**Bug encontrado por este teste e corrigido:** na primeira execução, 2 dos 3 processadores
**morreram** durante a queda. O `consumer.commit()` lançava `KafkaException`
(`COORDINATOR_NOT_AVAILABLE` / `REQUEST_TIMED_OUT`) e a exceção não era tratada. Corrigimos
capturando a exceção e registrando um aviso (`commit de offset falhou…`): o próximo commit
bem-sucedido cobre aqueles offsets e o banco descarta reprocessamentos. Com o teste repetido
(2 brokers fora por ~60 s), os 3 processadores sobreviveram, cada um com 2 partições, e voltaram
a processar com lag ≤ 5.

**Conclusão:** ⚠️ comportamento esperado para RF=3: o sistema tolera **1** falha de broker, não 2,
mas se recupera sozinho quando os brokers voltam.

### 2.5 Teste extra: reinício do PostgreSQL

Com `docker restart postgres`, os processadores registram `falha ao gravar lote`, reconectam
e gravam de novo o mesmo lote. Como o commit do offset só acontece depois da gravação,
nenhuma leitura se perde. ✅

### 2.6 Números gerais da execução

Ao fim das demonstrações (25/09/2026), o banco acumulava **234.098 leituras** processadas,
**11.841 alertas** (3.918 de temperatura, 3.920 de consumo, 4.003 de vibração) e **250 eventos
de rebalanço** registrados. Os totais incluem as execuções anteriores, porque o volume do
PostgreSQL é preservado entre elas.

**Verificação de perda de mensagens:** para cada uma das 6 partições, comparamos o menor e o
maior offset gravado no banco durante os testes com a quantidade de linhas. Não há nenhum
offset faltando: nenhuma leitura se perdeu nas quedas de broker, de consumidor e na
elasticidade.

```sql
SELECT kafka_partition, max(kafka_offset) - min(kafka_offset) + 1 - count(*) AS faltando
FROM leituras WHERE ts_processado > '2026-09-25 10:50' GROUP BY 1;   -- 0 em todas
```

---

## 3. O que funcionou e o que não funcionou

### Funcionou
- Cluster Kafka de 3 brokers (KRaft) com tópico de 6 partições e RF=3.
- Sensores como produtores em containers distintos, escaláveis com `--scale`.
- Balanceamento automático entre processadores do mesmo consumer group.
- Failover de broker: eleição de novo líder, sem perda de mensagens e sem parar o sistema.
- Failover de consumidor: rebalanço automático, com os sobreviventes assumindo as partições.
- Elasticidade com scale-up e scale-down, e o lag respondendo à mudança.
- Persistência sem duplicatas (idempotente), alertas no banco e no tópico `alertas`.
- Toda a configuração externalizada no `.env`; código com DocStrings.

### Limitações e o que não funcionou como gostaríamos
1. **Kubernetes não foi usado.** O enunciado permite Docker ou Kubernetes e optamos pelo
   Docker Compose. O escalonamento é manual (`--scale`), não há autoscaler baseado em lag
   como um HPA/KEDA faria no Kubernetes.
2. **Tolerância a só 1 broker fora** (§2.4). Para aguentar 2 falhas seriam necessários 5 nós
   e RF=5 (ou controllers separados).
3. **Detecção lenta de consumidor morto:** ~10 s (`session.timeout.ms`). Diminuir o valor
   acelera a detecção mas aumenta o risco de expulsar consumidores apenas lentos.
4. **Rebalanço "stop-the-world"** com a estratégia `range`: durante o rebalanço nenhum membro
   consome. A estratégia `cooperative-sticky` (configurável no `.env`) reduz esse efeito,
   mas não foi usada nos testes registrados.
5. **Liderança não volta na hora:** depois de religar o broker, os líderes continuam nos brokers
   que assumiram. O Kafka só devolve a liderança para a réplica preferida na checagem
   periódica (300 s por padrão).
6. **Distribuição desigual por partição:** o hash de 24 chaves não divide a carga de forma
   perfeitamente igual. Algumas partições recebem o dobro de outras (visível em `por_partição` nos logs).
7. **Semântica at-least-once**, não exactly-once, no Kafka: o tópico `alertas` pode receber
   um alerta duplicado se um lote for reprocessado. No banco a duplicata é evitada pela chave única.
8. **Demonstração visual** feita via Kafka UI e logs. Não construímos dashboard próprio.

---

## 4. Como reproduzir

```bash
make up                         # sobe tudo
make status                     # confere
make falha-broker BROKER=kafka1
make falha-consumidor
make elasticidade
make falha-dois-brokers
make clean                      # apaga tudo
```

## 5. Entregáveis e onde estão

| Entregável | Arquivo |
|---|---|
| Relatório | `RELATORIO.md` (este) |
| Documentação de instalação e uso | `README.md` |
| Makefile | `Makefile` |
| Código-fonte dos produtores e consumidores | `sensor/sensor.py`, `processador/processador.py` |
| pom.xml | não se aplica (Python). Dependências em `*/requirements.txt` |
| YAML com todos os serviços | `docker-compose.yml` |
| Scripts de simulação de falhas | `scripts/falha-broker.sh`, `scripts/falha-consumidor.sh`, `scripts/falha-dois-brokers.sh` |
| Logs mostrando rebalanço | `logs/falha-consumidor-*.log`, `logs/elasticidade-*.log`, `logs/eventos-rebalanco.csv` |
| Demonstração de elasticidade | `scripts/elasticidade.sh` + `logs/elasticidade-*.log` |
