import json
import os
import sys
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPICO_DERIVADO = 'eventos-crypto-derivados'
GRUPO_CONSUMIDOR = 'grupo-salvador-alertas'
ARQUIVO_ALERTAS = 'alertas.json'


def carregar_alertas_existentes():
    if os.path.exists(ARQUIVO_ALERTAS):
        try:
            with open(ARQUIVO_ALERTAS, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    return []


def salvar_alertas(alertas):
    with open(ARQUIVO_ALERTAS, 'w', encoding='utf-8') as f:
        json.dump(alertas, f, ensure_ascii=False, indent=2)

def remover_arquivo_alertas():
    """Apaga o arquivo de alertas temporário ao fechar o script."""
    if os.path.exists(ARQUIVO_ALERTAS):
        try:
            os.remove(ARQUIVO_ALERTAS)
            print(f"🗑️ Arquivo '{ARQUIVO_ALERTAS}' excluído com sucesso.")
        except Exception as e:
            print(f"⚠️ Erro ao excluir o arquivo '{ARQUIVO_ALERTAS}': {e}")

def iniciar_consumidor():
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GRUPO_CONSUMIDOR,
        'auto.offset.reset': 'earliest',  # Lê desde o início para não perder nada
        'enable.auto.commit': True
    }

    consumer = Consumer(conf)
    consumer.subscribe([TOPICO_DERIVADO])
    print(f"✅ Consumidor iniciado! Escutando o tópico '{TOPICO_DERIVADO}'...")

    alertas = carregar_alertas_existentes()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Erro no Kafka: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode('utf-8'))
                print(f"🚨 Novo alerta recebido: {payload.get('tipo_evento')}")

                # Insere o novo evento no início da lista
                alertas.insert(0, payload)

                # Mantém apenas os últimos 50 alertas no arquivo
                alertas = alertas[:50]
                salvar_alertas(alertas)
            except Exception as e:
                print(f"⚠️ Erro ao processar mensagem: {e}")

    except KeyboardInterrupt:
        print("\n🛑 Consumidor encerrado com sucesso.")
    finally:
        consumer.close()
        remover_arquivo_alertas()


if __name__ == "__main__":
    iniciar_consumidor()