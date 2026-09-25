"""
Sensor simulado de uma máquina da fábrica inteligente (produtor Kafka).

Cada container deste serviço representa um *gateway de sensores* que
monitora ``MACHINES_PER_SENSOR`` máquinas de um setor. Periodicamente ele
gera uma leitura (temperatura, vibração e consumo de energia) para cada
máquina e publica no tópico ``dados-sensores`` em formato JSON.

A chave da mensagem é o ID da máquina: o Kafka calcula
``hash(chave) % nº_partições``, então todas as leituras de uma mesma
máquina caem sempre na mesma partição (ordem preservada por máquina),
enquanto máquinas diferentes se espalham pelas partições.

Toda a configuração vem de variáveis de ambiente (ver ``.env``).
"""

import json
import logging
import os
import random
import signal
import socket
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


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
INTERVAL = env("SENSOR_INTERVAL_SECONDS", cast=float)
ANOMALY_PROB = env("SENSOR_ANOMALY_PROBABILITY", cast=float)
SECTORS = env("SENSOR_SECTORS").split(",")
MACHINES_PER_SENSOR = env("MACHINES_PER_SENSOR", cast=int)
STATS_EVERY = env("SENSOR_STATS_EVERY_SECONDS", cast=float)
LINGER_MS = env("PRODUCER_LINGER_MS", cast=int)
DELIVERY_TIMEOUT_MS = env("PRODUCER_DELIVERY_TIMEOUT_MS", cast=int)

# O hostname do container é único mesmo com "--scale", então serve de ID.
SENSOR_ID = f"sensor-{socket.gethostname()[:8]}"
SECTOR = SECTORS[sum(map(ord, SENSOR_ID)) % len(SECTORS)]

logging.basicConfig(
    level=env("LOG_LEVEL"),
    format=f"%(asctime)s [{SENSOR_ID}] %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("sensor")


class Maquina:
    """Estado simulado de uma máquina.

    Os valores fazem um passeio aleatório em torno de uma média, para
    parecer uma série temporal real e não ruído puro.

    :param machine_id: identificador único da máquina.
    """

    # (média, desvio do passo, mínimo, máximo) de cada grandeza em regime normal
    PERFIL = {
        "temperatura_c": (60.0, 1.5, 20.0, 80.0),
        "vibracao_mm_s": (3.0, 0.3, 0.5, 6.5),
        "consumo_kw": (25.0, 1.0, 5.0, 40.0),
    }

    def __init__(self, machine_id):
        self.machine_id = machine_id
        self.valores = {k: p[0] for k, p in self.PERFIL.items()}

    def ler(self):
        """Gera a próxima leitura da máquina.

        Com probabilidade ``SENSOR_ANOMALY_PROBABILITY`` uma das grandezas
        recebe um pico acima do limite normal, simulando uma falha.

        :return: dicionário com as três medidas.
        """
        leitura = {}
        for nome, (media, passo, minimo, maximo) in self.PERFIL.items():
            v = self.valores[nome] + random.gauss(0, passo) + (media - self.valores[nome]) * 0.1
            self.valores[nome] = min(max(v, minimo), maximo)
            leitura[nome] = round(self.valores[nome], 2)

        if random.random() < ANOMALY_PROB:
            nome = random.choice(list(self.PERFIL))
            leitura[nome] = round(self.PERFIL[nome][3] * random.uniform(1.15, 1.5), 2)
        return leitura


class Sensor:
    """Produtor Kafka que publica leituras das máquinas monitoradas."""

    def __init__(self):
        # acks=all + idempotência: a escrita só é confirmada quando todas
        # as réplicas em sincronia (ISR) gravaram; sem duplicatas em retry.
        # O cliente tenta reenviar sem limite de vezes (padrão do librdkafka)
        # até estourar delivery.timeout.ms.
        self.producer = Producer({
            "bootstrap.servers": BOOTSTRAP,
            "client.id": SENSOR_ID,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": LINGER_MS,
            "delivery.timeout.ms": DELIVERY_TIMEOUT_MS,
        })
        self.maquinas = [Maquina(f"{SECTOR}-{SENSOR_ID[7:]}-m{i}") for i in range(MACHINES_PER_SENSOR)]
        self.rodando = True
        self.enviadas = 0
        self.falhas = 0
        self.por_particao = {}

    def _on_delivery(self, err, msg):
        """Callback chamado pelo cliente Kafka quando o broker confirma (ou não) a mensagem.

        :param err: erro de entrega, ou ``None`` em caso de sucesso.
        :param msg: mensagem entregue (contém partição e offset).
        """
        if err is not None:
            self.falhas += 1
            log.warning("falha na entrega: %s", err)
        else:
            self.enviadas += 1
            self.por_particao[msg.partition()] = self.por_particao.get(msg.partition(), 0) + 1

    def parar(self, *_):
        """Handler de SIGTERM/SIGINT: encerra o loop principal."""
        log.info("sinal recebido, encerrando...")
        self.rodando = False

    def executar(self):
        """Loop principal: gera e publica leituras a cada ``SENSOR_INTERVAL_SECONDS``."""
        log.info("iniciado | setor=%s | máquinas=%s | tópico=%s | brokers=%s",
                 SECTOR, [m.machine_id for m in self.maquinas], TOPIC, BOOTSTRAP)
        ultimo_stats = time.monotonic()

        while self.rodando:
            for maquina in self.maquinas:
                evento = {
                    "sensor_id": SENSOR_ID,
                    "maquina": maquina.machine_id,
                    "setor": SECTOR,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    **maquina.ler(),
                }
                try:
                    self.producer.produce(TOPIC, key=maquina.machine_id,
                                          value=json.dumps(evento), on_delivery=self._on_delivery)
                except BufferError:
                    # Buffer local cheio (ex.: cluster indisponível): espera esvaziar
                    log.warning("buffer local cheio, aguardando o cluster...")
                    self.producer.poll(1)
            # poll() dispara os callbacks de entrega pendentes
            self.producer.poll(0)

            if time.monotonic() - ultimo_stats >= STATS_EVERY:
                log.info("enviadas=%d falhas=%d pendentes=%d por_partição=%s",
                         self.enviadas, self.falhas, len(self.producer),
                         dict(sorted(self.por_particao.items())))
                ultimo_stats = time.monotonic()
            time.sleep(INTERVAL)

        restantes = self.producer.flush(10)
        log.info("encerrado | enviadas=%d falhas=%d não_entregues=%d", self.enviadas, self.falhas, restantes)


if __name__ == "__main__":
    sensor = Sensor()
    signal.signal(signal.SIGTERM, sensor.parar)
    signal.signal(signal.SIGINT, sensor.parar)
    sensor.executar()
