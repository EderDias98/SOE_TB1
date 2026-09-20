import time
import logging
import json
from confluent_kafka import Producer
from coletores.bgp_collector import BGPCollector

# Configuração de logging estilizado
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] BGPProducer: %(message)s")
logger = logging.getLogger("BGPProducer")

# Endereços dos brokers expostos no localhost (Cluster multi-broker)
BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPICO_KAFKA = 'eventos-rede-primitivos'
INTERVALO_ENTRE_CICLOS = 30  # Tempo em segundos entre varreduras

# Lista de ASNs leves para evitar timeouts
LISTA_ASNS = [
    # Provedores e Órgãos Nacionais (Acadêmico/Governamental)
    "ASN1916",    # RNP - Rede Nacional de Ensino e Pesquisa
    "ASN1251",    # FAPESP
    "ASN26162",   # NIC.br
    "ASN27699",   # PRODEST (ES)
    "ASN28329",   # UOL / Universo Online
]


def callback_envio(err, msg):
    """Callback disparado pelo confluent_kafka ao confirmar entrega ou erro de publicação."""
    if err is not None:
        logger.error(f"❌ Falha no envio da mensagem: {err}")
    else:
        chave_str = msg.key().decode('utf-8') if msg.key() else "N/A"
        logger.info(
            f"📤 Mensagem enviada -> Tópico: '{msg.topic()}' | "
            f"Partição: [{msg.partition()}] | "
            f"Chave (ASN): {chave_str}"
        )


def executar_produtor():
    # Configuração do produtor via confluent_kafka
    conf = {
        'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
        'client.id': 'bgp-producer-host',
        'message.timeout.ms': 120000,

        # OTIMIZAÇÃO DE ALTA VAZÃO (Alta velocidade sem timeouts)
        'linger.ms': 5,  # Espera apenas 5ms para agrupar pacotes (MUITO mais rápido que 100ms)
        'batch.size': 131072,  # Lotes de 128 KB em memória
        'compression.type': 'snappy',  # Reduz tamanho da rede

        # NÍVEL DE CONFIRMAÇÃO (Velocidade vs Segurança)
        # '1' = O broker líder confirma o recebimento imediatamente (Rápido)
        # 'all' = Espera os 3 brokers replicarem no disco (Mais lento)
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

    coletor = BGPCollector(timeout=(3.0, 10.0), delay_entre_consultas=0.5)

    logger.info("🚀 Produtor BGP iniciado. Pressione Ctrl+C para encerrar.")

    try:
        while True:
            for asn in LISTA_ASNS:
                # Processa apenas o ASN atual
                for evento in coletor.coletar_fluxo_asn(asn):

                    if not evento or 'key' not in evento:
                        continue

                    chave = evento['key']
                    payload = evento['payload']

                    producer.produce(
                        topic=TOPICO_KAFKA,
                        key=chave.encode('utf-8'),
                        value=json.dumps(payload).encode('utf-8'),
                        callback=callback_envio
                    )

                    producer.poll(0)

                # Descarrega o buffer do Kafka especificamente para este ASN
                logger.info(f"🧹 Descarregando buffer do Kafka para o ASN {asn}...")
                producer.flush(timeout=60.0)

                # Pausa configurada entre requisições de ASNs
                time.sleep(coletor.delay)

            logger.info(f"😴 Ciclo completo finalizado. Aguardando {INTERVALO_ENTRE_CICLOS}s...\n")
            time.sleep(INTERVALO_ENTRE_CICLOS)

    except KeyboardInterrupt:
        logger.info("\n🛑 Encerramento solicitado pelo usuário.")
    finally:
        logger.info("Enviando mensagens pendentes no buffer...")
        producer.flush()
        logger.info("🏁 Produtor encerrado com sucesso.")


if __name__ == "__main__":
    executar_produtor()