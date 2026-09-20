import sys
import time
import logging
import json
from confluent_kafka import Producer
from crypto_collector import CryptoCollector  

# Configuração de logging unificada no stdout para evitar desalinhamento visual
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] CryptoProducer: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("CryptoProducer")

# Endereços dos brokers expostos no localhost (Cluster multi-broker)
BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPICO_KAFKA = 'eventos-crypto-primitivos'
INTERVALO_ENTRE_CICLOS = 0.1  # Tempo em segundos entre cada varredura completa das moedas

# Lista de pares de criptomoedas para monitoramento contínuo
LISTA_MOEDAS = [
    "BTCUSDT",  # Bitcoin
    "ETHUSDT",  # Ethereum
    "SOLUSDT",  # Solana
    "BNBUSDT",  # Binance Coin
    "ADAUSDT",  # Cardano
    "XRPUSDT",  # Ripple
]


def callback_envio(err, msg):
    """Callback disparado pelo confluent_kafka ao confirmar entrega ou erro de publicação."""
    if err is not None:
        logger.error(f"❌ Falha no envio da mensagem ao Kafka: {err}")
    else:
        chave_str = msg.key().decode('utf-8') if msg.key() else "N/A"
        logger.info(
            f"📤 Mensagem enviada -> Tópico: '{msg.topic()}' | "
            f"Partição: [{msg.partition()}] | "
            f"Chave (Moeda): {chave_str}"
        )


def executar_produtor():
    # Configuração do produtor via confluent_kafka
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'crypto-producer-host',
        'message.timeout.ms': 120000,

        # OTIMIZAÇÃO DE ALTA VAZÃO E BAIXA LATÊNCIA
        'linger.ms': 5,  # Espera apenas 5ms para agrupar mensagens antes do envio
        'batch.size': 131072,  # Lotes de até 128 KB em memória
        'compression.type': 'snappy',  # Compressão leve para rede

        # NÍVEL DE CONFIRMAÇÃO (1 = Confirmação do líder)
        'acks': 1,

        'queue.buffering.max.messages': 200000,
        'request.timeout.ms': 5000
    }

    try:
        producer = Producer(conf)
        logger.info(f"✅ Conectado aos brokers Kafka: {BOOTSTRAP_SERVERS}")
    except Exception as e:
        logger.error(f"❌ Falha ao inicializar o Confluent Producer: {e}")
        return

    coletor = CryptoCollector(timeout=(3.0, 10.0), delay_entre_consultas=0.01)

    logger.info("🚀 Produtor Cripto iniciado. Pressione Ctrl+C para encerrar.")

    try:
        while True:
            # Consome as cotações individuais geradas sem divisão por lotes
            for evento in coletor.coletar_fluxo_cripto(LISTA_MOEDAS):

                if not evento or 'key' not in evento:
                    continue

                chave = evento['key']
                payload = evento['payload']

                # Envia o evento primitivo individual para o tópico Kafka
                producer.produce(
                    topic=TOPICO_KAFKA,
                    key=chave.encode('utf-8'),
                    value=json.dumps(payload).encode('utf-8'),
                    callback=callback_envio
                )

                # Processa os callbacks da fila do Kafka sem bloquear a execução
                producer.poll(0)

            # Descarrega o buffer do Kafka após consultar toda a lista de moedas
            logger.info("🧹 Descarregando buffer do Kafka para a rodada atual...")
            producer.flush(timeout=10.0)

            logger.info(f"😴 Ciclo concluído. Aguardando {INTERVALO_ENTRE_CICLOS}s para nova varredura...\n")
            time.sleep(INTERVALO_ENTRE_CICLOS)

    except KeyboardInterrupt:
        logger.info("\n🛑 Encerramento solicitado pelo usuário.")
    finally:
        logger.info("Enviando mensagens pendentes no buffer...")
        producer.flush()
        logger.info("🏁 Produtor encerrado com sucesso.")


if __name__ == "__main__":
    executar_produtor()