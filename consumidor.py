import json
import logging
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
from analizadores.bgb_analizador import BGPAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] BGPProcessor: %(message)s"
)
logger = logging.getLogger("BGPProcessor")

# Brokers expostos no Docker Compose
BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPICO_KAFKA = 'eventos-rede-primitivos'
GROUP_ID = 'bgp-processor-group'


def executar_processador():
    """
    Consumidor Kafka principal que recebe os eventos do tópico e repassa ao BGPAnalyzer.
    """
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 10000,
        'max.poll.interval.ms': 300000
    }

    try:
        consumer = Consumer(conf)
        consumer.subscribe([TOPICO_KAFKA])
        logger.info(f"✅ Conectado aos brokers Kafka: {BOOTSTRAP_SERVERS}")
        logger.info(f"📡 Consumindo do tópico '{TOPICO_KAFKA}' no grupo '{GROUP_ID}'...")
    except Exception as e:
        logger.error(f"❌ Falha ao inicializar o Consumidor Kafka: {e}")
        sys.exit(1)

    bgp_analyzer = BGPAnalyzer()
    logger.info("🚀 Processador iniciado. Aguardando eventos BGP...\n")

    try:
        while True:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"❌ Erro na partição do Kafka: {msg.error()}")
                    continue

            # Tratamento defensivo de parsing da mensagem
            try:
                raw_payload = msg.value().decode('utf-8')
                payload = json.loads(raw_payload)
            except (UnicodeDecodeError, json.JSONDecodeError) as err:
                logger.error(f"❌ Falha ao decodificar JSON recebido: {err}")
                continue

            # Processa evento no analisador
            bgp_analyzer.analisar_evento(payload)

    except KeyboardInterrupt:
        logger.info("\n🛑 Solicitado encerramento do processador.")
    finally:
        logger.info("🔒 Fechando conexões e commitando offsets no Kafka...")
        consumer.close()
        logger.info("🏁 Processador encerrado com sucesso.")


if __name__ == "__main__":
    executar_processador()