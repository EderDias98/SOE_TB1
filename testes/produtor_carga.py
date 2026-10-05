"""
Teste de escalabilidade: produtor sintético que publica eventos PRECO_TICKER no mesmo tópico
do sistema, numa taxa controlada e com quantas moedas quiser (sem depender da API da Binance).
Os preços seguem um passeio aleatório; algumas vezes por minuto é injetado um salto para provocar
spikes, golden cross e anomalias de z-score nos consumidores.

Uso:
    python testes/produtor_carga.py --moedas 200 --taxa 2000 --duracao 60
"""
import time
import json
import random
import argparse
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
TOPICO_KAFKA = 'eventos-crypto-primitivos'


def main():
    parser = argparse.ArgumentParser(description="Produtor de carga sintética")
    parser.add_argument("--moedas", type=int, default=100, help="quantidade de moedas simuladas")
    parser.add_argument("--taxa", type=int, default=1000, help="mensagens por segundo (alvo)")
    parser.add_argument("--duracao", type=int, default=60, help="duração do teste em segundos")
    parser.add_argument("--volatilidade", type=float, default=0.0001,
                        help="desvio do preço por raiz de segundo (0.0001 ~ 0,08%% em 60 s, parecido com o mercado real)")
    parser.add_argument("--saltos-por-minuto", type=float, default=6, help="saltos de preço de 1%%-3%% por minuto (somando todas as moedas)")
    args = parser.parse_args()

    producer = Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'produtor-carga-teste',
        'linger.ms': 5,
        'batch.size': 131072,
        'compression.type': 'snappy',
        'acks': 1,
        'queue.buffering.max.messages': 500000,
    })

    moedas = [f"SIM{i:04d}USDT" for i in range(args.moedas)]
    # Cada moeda recebe taxa/moedas eventos por segundo; o desvio por evento é ajustado para que a
    # volatilidade no tempo seja a mesma em qualquer taxa (senão taxas altas gerariam alertas demais)
    desvio_por_evento = args.volatilidade * (args.moedas / args.taxa) ** 0.5
    prob_salto = args.saltos_por_minuto / 60 / args.taxa
    precos = {m: random.uniform(1, 1000) for m in moedas}
    erros = 0

    def callback(err, msg):
        nonlocal erros
        if err is not None:
            erros += 1

    print(f"Publicando ~{args.taxa} msg/s de {args.moedas} moedas por {args.duracao}s em '{TOPICO_KAFKA}'...")
    inicio = time.time()
    enviadas = 0
    ultimo_relatorio = inicio
    enviadas_no_ultimo = 0

    while time.time() - inicio < args.duracao:
        # Envia em pequenos blocos de 10 ms para manter a taxa alvo
        alvo_ate_agora = int((time.time() - inicio) * args.taxa)
        while enviadas < alvo_ate_agora:
            moeda = moedas[enviadas % len(moedas)]
            fator = random.gauss(0, desvio_por_evento)
            if random.random() < prob_salto:
                fator += random.choice([-1, 1]) * random.uniform(0.01, 0.03)
            precos[moeda] *= (1 + fator)

            payload = {"tipo_evento": "PRECO_TICKER", "moeda": moeda, "preco": precos[moeda], "timestamp": time.time()}
            try:
                producer.produce(TOPICO_KAFKA, key=moeda.encode('utf-8'), value=json.dumps(payload).encode('utf-8'), callback=callback)
            except BufferError:
                producer.poll(0.1)
                continue
            enviadas += 1
        producer.poll(0)

        agora = time.time()
        if agora - ultimo_relatorio >= 5:
            taxa_real = (enviadas - enviadas_no_ultimo) / (agora - ultimo_relatorio)
            print(f"  {agora - inicio:5.0f}s | enviadas: {enviadas:>8} | taxa: {taxa_real:8.0f} msg/s | erros: {erros}")
            ultimo_relatorio, enviadas_no_ultimo = agora, enviadas
        time.sleep(0.01)

    producer.flush(30)
    total = time.time() - inicio
    print(f"Fim: {enviadas} mensagens em {total:.1f}s ({enviadas / total:.0f} msg/s), erros de entrega: {erros}")


if __name__ == "__main__":
    main()
