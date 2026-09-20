import sys
import logging
import json
import time
from collections import deque, defaultdict
from confluent_kafka import Consumer, Producer, KafkaError

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

# Parâmetros de Regra de Negócio
JANELA_SPIKE_INDIVIDUAL = 30.0  # Janela para detectar variação na moeda (30s)
LIMIAR_SPIKE_PCT = 0.05  # Limiar de variação individual (|ΔP%| >= 1.0%)
JANELA_ESTRESSE_MERCADO = 120.0  # Janela de correlação composta (2 minutos)
MIN_MOEDAS_ESTRESSE = 2  # Mínimo de moedas em spike para estresse
COOLDOWN_ALERTA_SEGUNDOS = 30.0  # Pausa entre emissões de estresse para evitar spam


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


class ProcessadorEventosCompostos:
    """
    Atua como Consumidor de eventos primitivos e Produtor de eventos derivados.
    Inspeciona cotações, detecta spikes e infere estresse de mercado em tempo real.
    """

    def __init__(self, producer: Producer):
        self.producer = producer

        # Histórico de preços por moeda: { "BTCUSDT": deque([ (timestamp, preco) ]) }
        self.janelas_precos = defaultdict(deque)

        # Histórico de timestamps de spikes detectados: { "BTCUSDT": deque([ timestamp_spike ]) }
        self.historico_spikes = defaultdict(deque)

        # Controle de frequência de envio do alerta composto
        self.ultimo_alerta_estresse = 0.0

    def processar_evento(self, payload: dict):
        if not isinstance(payload, dict) or payload.get("tipo_evento") != "PRECO_TICKER":
            return

        moeda = payload.get("moeda")
        preco_atual = payload.get("preco")
        ts_atual = payload.get("timestamp")

        if not moeda or preco_atual is None or ts_atual is None:
            return

        # =====================================================================
        # ETAPA 1: DETECÇÃO DE SPIKE INDIVIDUAL DE VOLATILIDADE (SITUAÇÃO 1)
        # =====================================================================
        historico_p = self.janelas_precos[moeda]
        historico_p.append((ts_atual, preco_atual))

        # Purga preços anteriores a 30 segundos
        limite_inferior_p = ts_atual - JANELA_SPIKE_INDIVIDUAL
        while historico_p and historico_p[0][0] < limite_inferior_p:
            historico_p.popleft()

        spike_detectado = False
        if len(historico_p) >= 2:
            preco_inicial = historico_p[0][1]
            variacao_pct = ((preco_atual - preco_inicial) / preco_inicial) * 100.0

            if abs(variacao_pct) >= LIMIAR_SPIKE_PCT:
                spike_detectado = True
                # Registra o timestamp do spike na moeda
                self.historico_spikes[moeda].append(ts_atual)
                logger.warning(f"⚡ Spike detectado em {moeda}: {variacao_pct:+.2f}% em 30s!")

        # =====================================================================
        # ETAPA 2: CORRELAÇÃO DE MERCADO / INFERÊNCIA DO EVENTO COMPOSTO (2 MIN)
        # =====================================================================
        # Purga histórico de spikes mais antigos que 2 minutos (120s) para todas as moedas
        limite_inferior_estresse = ts_atual - JANELA_ESTRESSE_MERCADO
        moedas_em_estresse = []

        for m, dq_spikes in list(self.historico_spikes.items()):
            while dq_spikes and dq_spikes[0] < limite_inferior_estresse:
                dq_spikes.popleft()

            # Se a moeda teve pelo menos 1 spike nos últimos 2 minutos, contabiliza
            if len(dq_spikes) > 0:
                moedas_em_estresse.append(m)

        qtd_moedas_afetadas = len(moedas_em_estresse)

        # =====================================================================
        # ETAPA 3: PUBLICAÇÃO DO EVENTO DERIVADO NO KAFKA (ACKS=ALL)
        # =====================================================================
        if qtd_moedas_afetadas >= MIN_MOEDAS_ESTRESSE:
            # Garante que não enviaremos spam do mesmo alerta composto continuamente
            if (ts_atual - self.ultimo_alerta_estresse) >= COOLDOWN_ALERTA_SEGUNDOS:
                self.ultimo_alerta_estresse = ts_atual

                payload_derivado = {
                    "tipo_evento": "ALERTA_ESTRESSE_DE_MERCADO",
                    "nivel_severidade": "CRITICO",
                    "timestamp": ts_atual,
                    "janela_analise_segundos": JANELA_ESTRESSE_MERCADO,
                    "qtd_moedas_afetadas": qtd_moedas_afetadas,
                    "moedas_afetadas": moedas_em_estresse,
                    "descricao": f"Estresse detectado: {qtd_moedas_afetadas} moedas apresentaram spikes de volatilidade nos últimos 2 minutos."
                }

                logger.critical(
                    f"\n🔥 [EVENTO COMPOSTO INFERIDO - ESTRESSE DE MERCADO]\n"
                    f"   ├─ Severidade:       CRÍTICA\n"
                    f"   ├─ Moedas Afetadas:  {moedas_em_estresse}\n"
                    f"   ├─ Janela Temporal:  {JANELA_ESTRESSE_MERCADO}s (2 minutos)\n"
                    f"   └─ Ação:             Publicando no tópico '{TOPICO_DERIVADO}' com acks=all...\n"
                )

                # Publicação síncrona/garantida no tópico derivado
                self.producer.produce(
                    topic=TOPICO_DERIVADO,
                    key="MARKET_STRESS".encode('utf-8'),
                    value=json.dumps(payload_derivado).encode('utf-8'),
                    callback=callback_envio_derivado
                )

                # Descarrega o buffer do produtor imediatamente para garantir envio
                self.producer.flush(timeout=60.0)


def executar_processador():
    # 1. Configuração do Consumidor (Entrada)
    conf_consumer = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GRUPO_CONSUMIDOR,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }

    # 2. Configuração do Produtor Crítico (Saída - Zero Perda de Dados)
    conf_producer = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'crypto-composite-processor-producer',
        'acks': 'all',
        'enable.idempotence': True,  # Garante exatamente-uma entrega (sem duplicatas)
        'retries': 5,  # Tentativas em caso de oscilação de rede
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

    processador = ProcessadorEventosCompostos(producer=producer)

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
                processador.processar_evento(payload)
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