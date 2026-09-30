import sys
import logging
import json
from confluent_kafka import Consumer, Producer, KafkaError
from calculador_estresse_mercado import CalculadorEstresseMercado

# Configuração de logging padronizada no stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] CompositeProcessor: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("CompositeProcessor")

# Configurações de Rede Kafka
BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPICO_PRIMITIVO = 'eventos-crypto-primitivos'
TOPICO_DERIVADO = 'eventos-crypto-derivados'
GRUPO_CONSUMIDOR = 'grupo-processador-eventos-compostos'


def callback_envio_derivado(err, msg):
    """Callback de confirmação de entrega no tópico derivado."""
    if err is not None:
        logger.critical(f"❌ FALHA GRAVE ao publicar evento derivado no Kafka: {err}")
    else:
        chave_str = msg.key().decode('utf-8') if msg.key() else "N/A"
        logger.info(
            f"🛡️ [PRODUTOR CRÍTICO] Evento publicado com SUCESSO! | "
            f"Tópico: '{msg.topic()}' | Partição: [{msg.partition()}] | "
            f"Offset: {msg.offset()} | Chave: {chave_str}"
        )


def executar_processador():
    # 1. Configuração do Consumidor
    conf_consumer = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GRUPO_CONSUMIDOR,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }

    # 2. Configuração do Produtor Crítico
    conf_producer = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'crypto-composite-processor-producer',
        'acks': 'all',
        'enable.idempotence': True,
        'retries': 5,
        'max.in.flight.requests.per.connection': 5
    }

    try:
        consumer = Consumer(conf_consumer)
        consumer.subscribe([TOPICO_PRIMITIVO])
        logger.info(f"✅ Consumidor conectado aos brokers. Inscrito em: '{TOPICO_PRIMITIVO}'")

        producer = Producer(conf_producer)
        logger.info(f"✅ Produtor Crítico de Eventos Derivados inicializado (acks=all, idempotence=True)")
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar componentes Kafka: {e}")
        return

    # Inicializa o calculador isolado de regras de negócio
    calculador = CalculadorEstresseMercado(
        janela_spike_individual=30.0,
        limiar_spike_pct=0.05,
        janela_estresse_mercado=120.0,
        min_moedas_estresse=2,
        cooldown_alerta_segundos=30.0
    )

    logger.info("🎧 Processador de Eventos Compostos rodando... Pressione Ctrl+C para encerrar.\n")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"❌ Erro de leitura no Kafka: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode('utf-8'))

                # Executa o cálculo isolado
                alerta_derivado = calculador.processar_cotacao(payload)

                # Se o calculador inferiu um alerta de estresse, envia para o Kafka
                if alerta_derivado:
                    logger.critical(
                        f"\n🔥 [EVENTO COMPOSTO INFERIDO - ESTRESSE DE MERCADO]\n"
                        f"   ├─ Severidade:       {alerta_derivado['nivel_severidade']}\n"
                        f"   ├─ Moedas Afetadas:  {alerta_derivado['moedas_afetadas']}\n"
                        f"   ├─ Janela Temporal:  {alerta_derivado['janela_analise_segundos']}s\n"
                        f"   └─ Ação:             Publicando no tópico '{TOPICO_DERIVADO}' com acks=all...\n"
                    )

                    producer.produce(
                        topic=TOPICO_DERIVADO,
                        key="MARKET_STRESS".encode('utf-8'),
                        value=json.dumps(alerta_derivado).encode('utf-8'),
                        callback=callback_envio_derivado
                    )
                    producer.flush(timeout=60.0)

            except Exception as parse_err:
                logger.error(f"⚠️ Falha ao processar payload: {parse_err}")

    except KeyboardInterrupt:
        logger.info("\n🛑 Encerramento do processador solicitado.")
    finally:
        consumer.close()
        producer.flush()
        logger.info("🏁 Consumidor e Produtor encerrados.")


if __name__ == "__main__":
    executar_processador()