import sys
import logging
import json
import os
from confluent_kafka import Consumer, KafkaError
from monitor_cruzamento_medias import MonitorCruzamentoMedias

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] TrendConsumer: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("TrendConsumer")

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPICO_KAFKA = 'eventos-crypto-primitivos'
GRUPO_CONSUMIDOR = 'grupo-sit2-cruzamento-medias'
ARQUIVO_CRUZAMENTO = 'cruzamento_medias.json'

JANELA_RAPIDA_SEGUNDOS = 10.0
JANELA_LENTA_SEGUNDOS = 60.0


def carregar_eventos_existentes():
    if os.path.exists(ARQUIVO_CRUZAMENTO):
        try:
            with open(ARQUIVO_CRUZAMENTO, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    return []


def salvar_eventos(eventos):
    with open(ARQUIVO_CRUZAMENTO, 'w', encoding='utf-8') as f:
        json.dump(eventos, f, ensure_ascii=False, indent=2)


def remover_arquivo_cruzamento():
    """Apaga o arquivo JSON temporário ao fechar o script."""
    if os.path.exists(ARQUIVO_CRUZAMENTO):
        try:
            os.remove(ARQUIVO_CRUZAMENTO)
            logger.info(f"🗑️ Arquivo temporário '{ARQUIVO_CRUZAMENTO}' excluído com sucesso.")
        except Exception as e:
            logger.error(f"⚠️ Erro ao excluir o arquivo '{ARQUIVO_CRUZAMENTO}': {e}")


def executar_consumidor():
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GRUPO_CONSUMIDOR,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }

    try:
        consumer = Consumer(conf)
        consumer.subscribe([TOPICO_KAFKA])
        logger.info(f"✅ Consumidor conectado aos brokers: {BOOTSTRAP_SERVERS}")
        logger.info(f"📌 Inscrito no tópico: '{TOPICO_KAFKA}'")
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar o consumidor Kafka: {e}")
        return

    monitor = MonitorCruzamentoMedias(
        janela_rapida=JANELA_RAPIDA_SEGUNDOS,
        janela_lenta=JANELA_LENTA_SEGUNDOS
    )

    historico_eventos = carregar_eventos_existentes()

    logger.info("🎧 Aguardando eventos do Kafka para análise de tendência... Pressione Ctrl+C para encerrar.\n")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"❌ Erro de leitura Kafka: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode('utf-8'))
                alerta = monitor.processar_evento(payload)

                if alerta:
                    historico_eventos.insert(0, alerta)
                    historico_eventos = historico_eventos[:50]
                    salvar_eventos(historico_eventos)

            except Exception as parse_err:
                logger.error(f"⚠️ Falha ao decodificar payload JSON: {parse_err}")

    except KeyboardInterrupt:
        logger.info("\n🛑 Encerramento do consumidor solicitado...")
    finally:
        consumer.close()
        remover_arquivo_cruzamento()
        logger.info("🏁 Consumidor encerrado.")


if __name__ == "__main__":
    executar_consumidor()