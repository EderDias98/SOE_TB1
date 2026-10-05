import sys
import logging
import logging.handlers
import json
import os
import queue
import multiprocessing as mp
from confluent_kafka import Consumer, KafkaError
from monitor_volatidade_curta import MonitorVolatilidadeCurta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] VolatilityConsumer[%(processName)s]: %(message)s",
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger("VolatilityConsumer")

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPICO_KAFKA = 'eventos-crypto-primitivos'
GRUPO_CONSUMIDOR = 'grupo-sit1-volatilidade'
ARQUIVO_SPIKES = 'spikes_volatilidade.json'

JANELA_TEMPO_SEGUNDOS = 60.0
LIMIAR_VARIACAO_PCT = 0.1

# Um worker por partição do tópico (o tópico tem 3 partições).
# Como a chave da mensagem é a moeda, cada moeda sempre cai na mesma partição,
# então cada worker mantém sozinho a janela de preços das moedas que recebe.
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", 3))  # pode ser trocado pela variável de ambiente NUM_WORKERS


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


def configurar_log_worker(fila_logs):
    """Envia os logs do worker para o processo principal, que é quem escreve no stdout."""
    raiz = logging.getLogger()
    raiz.handlers.clear()
    raiz.addHandler(logging.handlers.QueueHandler(fila_logs))
    raiz.setLevel(logging.INFO)


def worker(id_worker, fila_alertas, fila_logs, parar):
    """Processo consumidor: lê as partições atribuídas a ele e envia os spikes para o processo principal."""
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

    monitor = MonitorVolatilidadeCurta(
        janela_segundos=JANELA_TEMPO_SEGUNDOS,
        limiar_pct=LIMIAR_VARIACAO_PCT
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
                alerta_spike = monitor.processar_evento(payload)

                if alerta_spike:
                    alerta_spike["worker"] = id_worker
                    alerta_spike["particao"] = msg.partition()
                    fila_alertas.put(alerta_spike)

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

    spikes_historico = carregar_spikes_existentes()

    logger.info("🎧 Aguardando cotações em tempo real... Pressione Ctrl+C para encerrar.\n")

    try:
        # O processo principal é o único que escreve no JSON lido pelo dashboard
        while True:
            try:
                alerta_spike = fila_alertas.get(timeout=1.0)
            except queue.Empty:
                if not any(p.is_alive() for p in workers):
                    logger.error("❌ Todos os workers pararam.")
                    break
                continue

            # Adiciona a detecção no topo do histórico
            spikes_historico.insert(0, alerta_spike)
            # Mantém apenas as últimas 50 detecções
            spikes_historico = spikes_historico[:50]
            salvar_spikes(spikes_historico)

    except KeyboardInterrupt:
        logger.info("\n🛑 Encerramento do consumidor solicitado...")
    finally:
        parar.set()
        for p in workers:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
        ouvinte_logs.stop()
        remover_arquivo_spikes()  # Limpa o arquivo JSON ao sair
        logger.info("🏁 Consumidor encerrado.")


if __name__ == "__main__":
    executar_consumidor()
