-- ===================================================================
-- Schema do banco da fábrica (executado 1x na criação do container).
-- ===================================================================

-- Toda leitura processada. As colunas kafka_* permitem provar, no
-- relatório, qual processador tratou qual partição e em que momento.
CREATE TABLE IF NOT EXISTS leituras (
    id               BIGSERIAL PRIMARY KEY,
    sensor_id        TEXT        NOT NULL,
    maquina          TEXT        NOT NULL,
    setor            TEXT        NOT NULL,
    temperatura_c    DOUBLE PRECISION,
    vibracao_mm_s    DOUBLE PRECISION,
    consumo_kw       DOUBLE PRECISION,
    ts_leitura       TIMESTAMPTZ NOT NULL,
    ts_processado    TIMESTAMPTZ NOT NULL DEFAULT now(),
    processador      TEXT        NOT NULL,
    kafka_partition  INT         NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    -- Evita duplicatas se uma mensagem for reprocessada após rebalanço
    -- (entrega "at-least-once" + insert idempotente).
    UNIQUE (kafka_partition, kafka_offset)
);

-- Anomalias detectadas (temperatura/vibração/consumo acima do limite).
CREATE TABLE IF NOT EXISTS alertas (
    id               BIGSERIAL PRIMARY KEY,
    sensor_id        TEXT        NOT NULL,
    maquina          TEXT        NOT NULL,
    setor            TEXT        NOT NULL,
    tipo             TEXT        NOT NULL,   -- TEMPERATURA | VIBRACAO | CONSUMO
    valor            DOUBLE PRECISION NOT NULL,
    limite           DOUBLE PRECISION NOT NULL,
    ts_leitura       TIMESTAMPTZ NOT NULL,
    ts_detectado     TIMESTAMPTZ NOT NULL DEFAULT now(),
    processador      TEXT        NOT NULL,
    kafka_partition  INT         NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    UNIQUE (kafka_partition, kafka_offset, tipo)
);

-- Histórico de rebalanços: cada vez que um processador recebe ou
-- perde partições, uma linha é gravada aqui.
CREATE TABLE IF NOT EXISTS eventos_rebalanco (
    id           BIGSERIAL PRIMARY KEY,
    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
    processador  TEXT        NOT NULL,
    evento       TEXT        NOT NULL,   -- ASSIGN | REVOKE | LOST
    particoes    INT[]       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leituras_ts    ON leituras (ts_processado);
CREATE INDEX IF NOT EXISTS idx_leituras_proc  ON leituras (processador);
CREATE INDEX IF NOT EXISTS idx_alertas_ts     ON alertas (ts_detectado);
