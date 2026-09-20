import sys
import logging
import json
import os
from confluent_kafka import Consumer, KafkaError
from monitor_volatidade_curta import MonitorVolatilidadeCurta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] VolatilityConsumer: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("VolatilityConsumer")

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPICO_KAFKA = 'eventos-crypto-primitivos'
GRUPO_CONSUMIDOR = 'grupo-sit1-volatilidade'
ARQUIVO_SPIKES = 'spikes_volatilidade.json'

JANELA_TEMPO_SEGUNDOS = 60.0
LIMIAR_VARIACAO_PCT = 0.05


def carregar_spikes_existentes():
    if os.path.exists(ARQUIVO_SPIKES):
        try:
            with open(ARQUIVO_SPIKES, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    return []


def salvar_spikes(spikes):
    with open(ARQUIVO_SPIKES, 'w', encoding='utf-8') as f:
        json.dump(spikes, f, ensure_ascii=False, indent=2)


def remover_arquivo_spikes():
    """Apaga o arquivo JSON temporário ao fechar o script."""
    if os.path.exists(ARQUIVO_SPIKES):
        try:
            os.remove(ARQUIVO_SPIKES)
            logger.info(f"🗑️ Arquivo temporário '{ARQUIVO_SPIKES}' excluído com sucesso.")
        except Exception as e:
            logger.error(f"⚠️ Erro ao excluir o arquivo '{ARQUIVO_SPIKES}': {e}")


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

    monitor = MonitorVolatilidadeCurta(
        janela_segundos=JANELA_TEMPO_SEGUNDOS,
        limiar_pct=LIMIAR_VARIACAO_PCT
    )

    spikes_historico = carregar_spikes_existentes()

    logger.info("🎧 Aguardando cotações em tempo real... Pressione Ctrl+C para encerrar.\n")

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
                alerta_spike = monitor.processar_evento(payload)

                if alerta_spike:
                    # Adiciona a detecção no topo do histórico
                    spikes_historico.insert(0, alerta_spike)
                    # Mantém apenas as últimas 50 detecções
                    spikes_historico = spikes_historico[:50]
                    salvar_spikes(spikes_historico)

            except Exception as parse_err:
                logger.error(f"⚠️ Falha ao decodificar payload JSON: {parse_err}")

    except KeyboardInterrupt:
        logger.info("\n🛑 Encerramento do consumidor solicitado...")
    finally:
        consumer.close()
        remover_arquivo_spikes()  # Limpa o arquivo JSON ao sair
        logger.info("🏁 Consumidor encerrado.")


if __name__ == "__main__":
    executar_consumidor()