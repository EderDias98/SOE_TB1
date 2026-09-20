import sys
import logging
import json
import os
from confluent_kafka import Consumer, KafkaError
from monitor_anomalia_zscore import MonitorAnomaliaZScore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] ZScoreConsumer: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("ZScoreConsumer")

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPICO_KAFKA = 'eventos-crypto-primitivos'
GRUPO_CONSUMIDOR = 'grupo-sit3-zscore'
ARQUIVO_ANOMALIAS = 'anomalias_zscore.json'

JANELA_TEMPO_SEGUNDOS = 60.0
LIMIAR_ZSCORE = 2.5
MIN_AMOSTRAS_REQUERIDAS = 5


def carregar_anomalias_existentes():
    if os.path.exists(ARQUIVO_ANOMALIAS):
        try:
            with open(ARQUIVO_ANOMALIAS, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    return []


def salvar_anomalias(anomalias):
    with open(ARQUIVO_ANOMALIAS, 'w', encoding='utf-8') as f:
        json.dump(anomalias, f, ensure_ascii=False, indent=2)


def remover_arquivo_anomalias():
    """Apaga o arquivo JSON temporário ao fechar o script."""
    if os.path.exists(ARQUIVO_ANOMALIAS):
        try:
            os.remove(ARQUIVO_ANOMALIAS)
            logger.info(f"🗑️ Arquivo temporário '{ARQUIVO_ANOMALIAS}' excluído com sucesso.")
        except Exception as e:
            logger.error(f"⚠️ Erro ao excluir o arquivo '{ARQUIVO_ANOMALIAS}': {e}")


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

    monitor = MonitorAnomaliaZScore(
        janela_segundos=JANELA_TEMPO_SEGUNDOS,
        limiar_z=LIMIAR_ZSCORE,
        min_amostras=MIN_AMOSTRAS_REQUERIDAS
    )

    historico_anomalias = carregar_anomalias_existentes()

    logger.info("🎧 Aguardando eventos do Kafka para detecção de anomalias estatísticas... Pressione Ctrl+C para encerrar.\n")

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
                    historico_anomalias.insert(0, alerta)
                    historico_anomalias = historico_anomalias[:50]
                    salvar_anomalias(historico_anomalias)

            except Exception as parse_err:
                logger.error(f"⚠️ Falha ao decodificar payload JSON: {parse_err}")

    except KeyboardInterrupt:
        logger.info("\n🛑 Encerramento do consumidor solicitado...")
    finally:
        consumer.close()
        remover_arquivo_anomalias()
        logger.info("🏁 Consumidor encerrado.")


if __name__ == "__main__":
    executar_consumidor()