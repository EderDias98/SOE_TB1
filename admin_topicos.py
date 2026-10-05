import os
import sys
import time
import logging
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigEntry, AlterConfigOpType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] TopicAdmin: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("TopicAdmin")

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

# Pode ser trocado pela variável de ambiente NUM_PARTICOES (o Kafka só permite aumentar)
NUM_PARTICOES = int(os.environ.get("NUM_PARTICOES", 3))
FATOR_REPLICACAO = 3

# Tempo que as mensagens ficam retidas em cada tópico (retention.ms)
RETENCAO_PRIMITIVOS_MS = 60 * 60 * 1000        # 1 hora
RETENCAO_DERIVADOS_MS = 24 * 60 * 60 * 1000    # 24 horas

TOPICOS = {
    'eventos-crypto-primitivos': {
        'retention.ms': str(RETENCAO_PRIMITIVOS_MS),
        'min.insync.replicas': '2',
    },
    'eventos-crypto-derivados': {
        'retention.ms': str(RETENCAO_DERIVADOS_MS),
        'min.insync.replicas': '2',
    },
}


def atualizar_configuracao(admin, nome_topico, config):
    """Aplica as configurações em um tópico que já existe (ex.: alterar a retenção)."""
    recurso = ConfigResource(
        ConfigResource.Type.TOPIC,
        nome_topico,
        incremental_configs=[
            ConfigEntry(chave, valor, incremental_operation=AlterConfigOpType.SET)
            for chave, valor in config.items()
        ]
    )
    for _, futuro in admin.incremental_alter_configs([recurso]).items():
        futuro.result()
    logger.info(f"🔧 Tópico '{nome_topico}' já existia. Configurações atualizadas: {config}")


def aumentar_particoes(admin, nome_topico):
    """Aumenta o número de partições de um tópico existente até NUM_PARTICOES (se necessário)."""
    atuais = len(admin.list_topics(nome_topico, timeout=10).topics[nome_topico].partitions)
    if atuais < NUM_PARTICOES:
        admin.create_partitions([NewPartitions(nome_topico, NUM_PARTICOES)])[nome_topico].result()
        logger.info(f"➕ Tópico '{nome_topico}': partições aumentadas de {atuais} para {NUM_PARTICOES}")
    elif atuais > NUM_PARTICOES:
        logger.warning(f"⚠️ Tópico '{nome_topico}' tem {atuais} partições; o Kafka não permite reduzir para {NUM_PARTICOES}")


def criar_ou_atualizar_topico(admin, nome, config):
    novo = NewTopic(nome, num_partitions=NUM_PARTICOES, replication_factor=FATOR_REPLICACAO, config=config)
    try:
        admin.create_topics([novo])[nome].result()
        logger.info(f"✅ Tópico '{nome}' criado: {NUM_PARTICOES} partições, RF={FATOR_REPLICACAO}, config={config}")
    except KafkaException as e:
        if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            atualizar_configuracao(admin, nome, config)
            aumentar_particoes(admin, nome)
        else:
            raise


def garantir_topicos(tentativas=10, espera_segundos=3.0):
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})

    for nome, config in TOPICOS.items():
        # Logo após o docker compose up, os 3 brokers podem ainda não estar registrados
        # (erro de fator de replicação), então tenta novamente algumas vezes.
        for tentativa in range(1, tentativas + 1):
            try:
                criar_ou_atualizar_topico(admin, nome, config)
                break
            except KafkaException as e:
                if tentativa == tentativas:
                    logger.error(f"❌ Erro ao configurar o tópico '{nome}': {e}")
                    raise
                logger.warning(f"⏳ Cluster ainda não está pronto ({e}). Tentativa {tentativa}/{tentativas}...")
                time.sleep(espera_segundos)


if __name__ == "__main__":
    garantir_topicos()
