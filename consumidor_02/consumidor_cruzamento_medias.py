import sys
import logging
import logging.handlers
import json
import os
import queue
import multiprocessing as mp
from confluent_kafka import Consumer, KafkaError
from monitor_cruzamento_medias import MonitorCruzamentoMedias

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] TrendConsumer[%(processName)s]: %(message)s",
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger("TrendConsumer")

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPICO_KAFKA = 'eventos-crypto-primitivos'
GRUPO_CONSUMIDOR = 'grupo-sit2-cruzamento-medias'
ARQUIVO_CRUZAMENTO = 'cruzamento_medias.json'

JANELA_RAPIDA_SEGUNDOS = 10.0
JANELA_LENTA_SEGUNDOS = 60.0

# Um worker por partição do tópico (o tópico tem 3 partições).
# Como a chave da mensagem é a moeda, cada moeda sempre cai na mesma partição,
# então cada worker mantém sozinho as médias móveis das moedas que recebe.
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", 3))  # pode ser trocado pela variável de ambiente NUM_WORKERS


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


def configurar_log_worker(fila_logs):
    """Envia os logs do worker para o processo principal, que é quem escreve no stdout."""
    raiz = logging.getLogger()
    raiz.handlers.clear()
    raiz.addHandler(logging.handlers.QueueHandler(fila_logs))
    raiz.setLevel(logging.INFO)


def worker(id_worker, fila_alertas, fila_logs, parar):
    """Processo consumidor: lê as partições atribuídas a ele e envia os cruzamentos para o processo principal."""
    configurar_log_worker(fila_logs)

    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GRUPO_CONSUMIDOR,
        'client.id': f'{GRUPO_CONSUMIDOR}-w{id_worker}',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }

    def ao_atribuir(consumer, particoes):
        logger.info(f"🧩 Partições atribuídas: {[p.partition for p in particoes]}")

    def ao_revogar(consumer, particoes):
        logger.info(f"↩️ Partições revogadas: {[p.partition for p in particoes]}")

    try:
        consumer = Consumer(conf)
        consumer.subscribe([TOPICO_KAFKA], on_assign=ao_atribuir, on_revoke=ao_revogar)
        logger.info(f"✅ Worker {id_worker} inscrito no tópico '{TOPICO_KAFKA}' (grupo '{GRUPO_CONSUMIDOR}')")
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar o consumidor Kafka: {e}")
        return

    monitor = MonitorCruzamentoMedias(
        janela_rapida=JANELA_RAPIDA_SEGUNDOS,
        janela_lenta=JANELA_LENTA_SEGUNDOS
    )
    processo_pai = mp.parent_process()

    try:
        while not parar.is_set() and processo_pai.is_alive():
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
                    alerta["worker"] = id_worker
                    alerta["particao"] = msg.partition()
                    fila_alertas.put(alerta)

            except Exception as parse_err:
                logger.error(f"⚠️ Falha ao decodificar payload JSON: {parse_err}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        logger.info(f"🏁 Worker {id_worker} encerrado.")


def executar_consumidor():
    fila_alertas = mp.Queue()
    fila_logs = mp.Queue()
    parar = mp.Event()

    ouvinte_logs = logging.handlers.QueueListener(fila_logs, *logging.getLogger().handlers)
    ouvinte_logs.start()

    workers = [
        mp.Process(target=worker, args=(i, fila_alertas, fila_logs, parar), name=f"worker-{i}", daemon=True)
        for i in range(1, NUM_WORKERS + 1)
    ]
    for p in workers:
        p.start()
    logger.info(f"🚀 {NUM_WORKERS} workers iniciados no grupo '{GRUPO_CONSUMIDOR}'")

    historico_eventos = carregar_eventos_existentes()

    logger.info("🎧 Aguardando eventos do Kafka para análise de tendência... Pressione Ctrl+C para encerrar.\n")

    try:
        # O processo principal é o único que escreve no JSON lido pelo dashboard
        while True:
            try:
                alerta = fila_alertas.get(timeout=1.0)
            except queue.Empty:
                if not any(p.is_alive() for p in workers):
                    logger.error("❌ Todos os workers pararam.")
                    break
                continue

            historico_eventos.insert(0, alerta)
            historico_eventos = historico_eventos[:50]
            salvar_eventos(historico_eventos)

    except KeyboardInterrupt:
        logger.info("\n🛑 Encerramento do consumidor solicitado...")
    finally:
        parar.set()
        for p in workers:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
        ouvinte_logs.stop()
        remover_arquivo_cruzamento()
        logger.info("🏁 Consumidor encerrado.")


if __name__ == "__main__":
    executar_consumidor()
