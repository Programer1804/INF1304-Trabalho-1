"""
Processador de dados dos sensores (consumidor Kafka).

Várias instâncias deste serviço rodam com o mesmo ``group.id``. O Kafka
distribui as partições do tópico ``dados-sensores`` entre elas: cada
partição é lida por exatamente um processador do grupo. Quando um
processador entra ou sai (queda, scale up/down), o coordenador do grupo
faz um **rebalanço** e redistribui as partições. Este programa registra
cada rebalanço no log e na tabela ``eventos_rebalanco``.

Para cada leitura recebida o processador:
  1. verifica se temperatura, vibração ou consumo passaram do limite;
  2. grava a leitura (e eventuais alertas) no PostgreSQL;
  3. publica os alertas no tópico ``alertas``;
  4. só então faz commit do offset (entrega *at-least-once*).

Toda a configuração vem de variáveis de ambiente (ver ``.env``).
"""

import json
import logging
import os
import signal
import socket
import sys
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer


def env(name, default=None, cast=str):
    """Lê uma variável de ambiente e converte para o tipo desejado.

    :param name: nome da variável.
    :param default: valor usado se a variável não existir; se ``None``
        a variável é obrigatória.
    :param cast: função de conversão (``str``, ``int``, ``float``...).
    :return: valor convertido.
    :raises SystemExit: se a variável for obrigatória e estiver ausente.
    """
    value = os.getenv(name, default)
    if value is None:
        sys.exit(f"variável de ambiente obrigatória ausente: {name}")
    return cast(value)


BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = env("TOPIC_NAME")
ALERT_TOPIC = env("ALERT_TOPIC_NAME")
GROUP_ID = env("CONSUMER_GROUP_ID")
STRATEGY = env("PARTITION_ASSIGNMENT_STRATEGY")
SESSION_TIMEOUT_MS = env("SESSION_TIMEOUT_MS", cast=int)
HEARTBEAT_MS = env("HEARTBEAT_INTERVAL_MS", cast=int)
PROCESSING_DELAY_S = env("PROCESSING_DELAY_MS", cast=int) / 1000
BATCH_SIZE = env("CONSUMER_BATCH_SIZE", cast=int)
STATS_EVERY = env("CONSUMER_STATS_EVERY_SECONDS", cast=float)
AUTO_OFFSET_RESET = env("CONSUMER_AUTO_OFFSET_RESET")

LIMITES = {
    "TEMPERATURA": ("temperatura_c", env("LIMIT_TEMPERATURE_C", cast=float)),
    "VIBRACAO": ("vibracao_mm_s", env("LIMIT_VIBRATION_MM_S", cast=float)),
    "CONSUMO": ("consumo_kw", env("LIMIT_POWER_KW", cast=float)),
}

DB_DSN = {
    "host": env("POSTGRES_HOST"),
    "port": env("POSTGRES_PORT", cast=int),
    "dbname": env("POSTGRES_DB"),
    "user": env("POSTGRES_USER"),
    "password": env("POSTGRES_PASSWORD"),
}

PROC_ID = f"proc-{socket.gethostname()[:8]}"

logging.basicConfig(
    level=env("LOG_LEVEL"),
    format=f"%(asctime)s [{PROC_ID}] %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("processador")


class Repositorio:
    """Acesso ao PostgreSQL com reconexão automática."""

    def __init__(self):
        self.conn = None
        self._conectar()

    def _conectar(self):
        """Abre a conexão, tentando de novo até o banco responder."""
        while True:
            try:
                self.conn = psycopg2.connect(**DB_DSN)
                self.conn.autocommit = False
                log.info("conectado ao PostgreSQL em %s:%s", DB_DSN["host"], DB_DSN["port"])
                return
            except psycopg2.OperationalError as e:
                log.warning("PostgreSQL indisponível (%s), nova tentativa em 2s", e)
                time.sleep(2)

    def salvar(self, leituras, alertas):
        """Grava um lote de leituras e alertas numa única transação.

        ``ON CONFLICT DO NOTHING`` torna a gravação idempotente: se uma
        mensagem for reprocessada após um rebalanço, não duplica.

        :param leituras: lista de tuplas no formato da tabela ``leituras``.
        :param alertas: lista de tuplas no formato da tabela ``alertas``.
        """
        try:
            with self.conn.cursor() as cur:
                if leituras:
                    execute_values(cur, """
                        INSERT INTO leituras (sensor_id, maquina, setor, temperatura_c, vibracao_mm_s,
                               consumo_kw, ts_leitura, processador, kafka_partition, kafka_offset)
                        VALUES %s ON CONFLICT DO NOTHING""", leituras)
                if alertas:
                    execute_values(cur, """
                        INSERT INTO alertas (sensor_id, maquina, setor, tipo, valor, limite, ts_leitura,
                               processador, kafka_partition, kafka_offset)
                        VALUES %s ON CONFLICT DO NOTHING""", alertas)
            self.conn.commit()
        except psycopg2.Error:
            # conexão caiu ou transação falhou: reabre e deixa o chamador tentar de novo
            self._reabrir()
            raise

    def _reabrir(self):
        """Descarta a conexão atual (possivelmente quebrada) e abre outra."""
        try:
            self.conn.close()
        except psycopg2.Error:
            pass
        self._conectar()

    def registrar_rebalanco(self, evento, particoes):
        """Registra na tabela ``eventos_rebalanco`` que este processador ganhou/perdeu partições.

        :param evento: ``ASSIGN``, ``REVOKE`` ou ``LOST``.
        :param particoes: lista com os números das partições envolvidas.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("INSERT INTO eventos_rebalanco (processador, evento, particoes) VALUES (%s, %s, %s)",
                            (PROC_ID, evento, particoes))
            self.conn.commit()
        except psycopg2.Error as e:
            log.warning("não foi possível registrar rebalanço: %s", e)
            self._reabrir()


class Processador:
    """Consumidor Kafka que detecta anomalias nas leituras dos sensores."""

    def __init__(self):
        self.repo = Repositorio()
        self.consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "client.id": PROC_ID,
            "partition.assignment.strategy": STRATEGY,
            "session.timeout.ms": SESSION_TIMEOUT_MS,
            "heartbeat.interval.ms": HEARTBEAT_MS,
            # offset só é salvo depois que a leitura está no banco
            "enable.auto.commit": False,
            "auto.offset.reset": AUTO_OFFSET_RESET,
        })
        self.alert_producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all",
                                        "client.id": f"{PROC_ID}-alertas"})
        self.particoes = set()
        self.rodando = True
        self.total = 0
        self.total_alertas = 0
        self.por_particao = {}

    # ------------------------------------------------------------------
    # Callbacks de rebalanço (chamados pelo cliente dentro de consume())
    # ------------------------------------------------------------------
    def _on_assign(self, consumer, parts):
        """Chamado quando o coordenador do grupo entrega partições a este processador.

        :param consumer: o próprio consumidor.
        :param parts: lista de ``TopicPartition`` recebidas.
        """
        novas = sorted(p.partition for p in parts)
        self.particoes |= set(novas)
        log.info(">>> REBALANÇO: RECEBIDAS partições %s | agora responsável por %s",
                 novas, sorted(self.particoes))
        self.repo.registrar_rebalanco("ASSIGN", novas)

    def _on_revoke(self, consumer, parts):
        """Chamado antes de este processador perder partições (rebalanço normal).

        Faz commit síncrono do que já foi processado, para que o próximo
        dono da partição continue exatamente de onde paramos.

        :param consumer: o próprio consumidor.
        :param parts: lista de ``TopicPartition`` que serão retiradas.
        """
        perdidas = sorted(p.partition for p in parts)
        try:
            consumer.commit(asynchronous=False)
        except KafkaException:
            pass  # nada a commitar
        self.particoes -= set(perdidas)
        log.info("<<< REBALANÇO: REVOGADAS partições %s | restam %s", perdidas, sorted(self.particoes))
        self.repo.registrar_rebalanco("REVOKE", perdidas)

    def _on_lost(self, consumer, parts):
        """Chamado quando as partições foram perdidas sem aviso (ex.: sessão expirou).

        :param consumer: o próprio consumidor.
        :param parts: lista de ``TopicPartition`` perdidas.
        """
        perdidas = sorted(p.partition for p in parts)
        self.particoes -= set(perdidas)
        log.warning("!!! REBALANÇO: PERDIDAS partições %s (sessão expirou)", perdidas)
        self.repo.registrar_rebalanco("LOST", perdidas)

    # ------------------------------------------------------------------
    def _analisar(self, evento):
        """Compara as medidas da leitura com os limites configurados.

        :param evento: leitura já decodificada do JSON.
        :return: lista de ``(tipo, valor, limite)`` para cada limite ultrapassado.
        """
        return [(tipo, evento[campo], limite)
                for tipo, (campo, limite) in LIMITES.items()
                if evento.get(campo) is not None and evento[campo] > limite]

    def _publicar_alerta(self, chave, valor):
        """Publica um alerta no tópico de alertas.

        Se o buffer local do produtor estiver cheio (ex.: cluster
        indisponível), espera ele esvaziar e tenta de novo, em vez de
        deixar o ``BufferError`` derrubar o processador.

        :param chave: chave da mensagem (ID da máquina).
        :param valor: alerta serializado em JSON.
        """
        while True:
            try:
                self.alert_producer.produce(ALERT_TOPIC, key=chave, value=valor)
                return
            except BufferError:
                log.warning("buffer de alertas cheio, aguardando o cluster...")
                self.alert_producer.poll(1)

    def _processar_lote(self, msgs):
        """Processa um lote de mensagens: analisa, grava, publica alertas e faz commit.

        :param msgs: mensagens retornadas por ``consumer.consume()``.
        """
        leituras, alertas = [], []
        for msg in msgs:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("erro de consumo: %s", msg.error())
                continue
            try:
                ev = json.loads(msg.value())
                ts = datetime.fromisoformat(ev["timestamp"])
            except (ValueError, KeyError) as e:
                log.warning("mensagem inválida em p%d@%d ignorada: %s", msg.partition(), msg.offset(), e)
                continue

            if PROCESSING_DELAY_S:
                time.sleep(PROCESSING_DELAY_S)

            p, o = msg.partition(), msg.offset()
            leituras.append((ev["sensor_id"], ev["maquina"], ev["setor"], ev.get("temperatura_c"),
                             ev.get("vibracao_mm_s"), ev.get("consumo_kw"), ts, PROC_ID, p, o))
            for tipo, valor, limite in self._analisar(ev):
                alertas.append((ev["sensor_id"], ev["maquina"], ev["setor"], tipo, valor, limite,
                                ts, PROC_ID, p, o))
                log.warning("ALERTA %s | máquina=%s setor=%s valor=%.2f limite=%.2f (p%d@%d)",
                            tipo, ev["maquina"], ev["setor"], valor, limite, p, o)
                self._publicar_alerta(ev["maquina"], json.dumps(
                    {"tipo": tipo, "valor": valor, "limite": limite, "processador": PROC_ID, **ev}))
            self.por_particao[p] = self.por_particao.get(p, 0) + 1

        if not leituras:
            return
        while True:
            try:
                self.repo.salvar(leituras, alertas)
                break
            except psycopg2.Error as e:
                log.warning("falha ao gravar lote (%s), tentando de novo", e)
        self.alert_producer.poll(0)
        # commit dos offsets só depois de persistir (at-least-once).
        # Se o cluster estiver indisponível o commit falha, mas não derrubamos o
        # processador: o próximo commit bem-sucedido cobre estes offsets e, se
        # houver reprocessamento, o banco descarta as duplicatas (UNIQUE).
        try:
            self.consumer.commit(asynchronous=False)
        except KafkaException as e:
            log.warning("commit de offset falhou (%s); será refeito no próximo lote", e)
        self.total += len(leituras)
        self.total_alertas += len(alertas)

    def parar(self, *_):
        """Handler de SIGTERM/SIGINT: encerra o loop principal de forma limpa."""
        log.info("sinal recebido, saindo do grupo...")
        self.rodando = False

    def executar(self):
        """Loop principal de consumo."""
        log.info("iniciado | grupo=%s | tópico=%s | estratégia=%s | brokers=%s",
                 GROUP_ID, TOPIC, STRATEGY, BOOTSTRAP)
        self.consumer.subscribe([TOPIC], on_assign=self._on_assign,
                                on_revoke=self._on_revoke, on_lost=self._on_lost)
        ultimo_stats = time.monotonic()
        try:
            while self.rodando:
                msgs = self.consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
                if msgs:
                    self._processar_lote(msgs)
                if time.monotonic() - ultimo_stats >= STATS_EVERY:
                    log.info("processadas=%d alertas=%d partições=%s por_partição=%s",
                             self.total, self.total_alertas, sorted(self.particoes),
                             dict(sorted(self.por_particao.items())))
                    ultimo_stats = time.monotonic()
        finally:
            # close() dispara on_revoke e avisa o coordenador: rebalanço imediato
            self.consumer.close()
            self.alert_producer.flush(5)
            log.info("encerrado | processadas=%d alertas=%d", self.total, self.total_alertas)


if __name__ == "__main__":
    proc = Processador()
    signal.signal(signal.SIGTERM, proc.parar)
    signal.signal(signal.SIGINT, proc.parar)
    proc.executar()
