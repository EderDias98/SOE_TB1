"""
Mede, a cada N segundos, o LAG (mensagens no tópico ainda não consumidas) e a vazão de consumo
(mensagens/s) de cada grupo de consumidores do sistema. Se o lag cresce sem parar, os
consumidores não estão dando conta da carga; se fica estável perto de zero, estão.

Uso:
    python testes/medir_lag.py            -> mede a cada 5 s até Ctrl+C
    python testes/medir_lag.py 2 grupo-sit1-volatilidade
"""
import sys
import time
from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

GRUPOS = {
    'grupo-sit1-volatilidade': 'eventos-crypto-primitivos',
    'grupo-sit2-cruzamento-medias': 'eventos-crypto-primitivos',
    'grupo-sit3-zscore': 'eventos-crypto-primitivos',
    'grupo-processador-eventos-compostos': 'eventos-crypto-primitivos',
    'grupo-salvador-alertas': 'eventos-crypto-derivados',
}


def medir_grupo(grupo, topico):
    """Retorna (total_commitado, lag_total, lag_por_particao) do grupo no tópico."""
    # Consumer só para consulta: não se inscreve no tópico, então não entra no grupo
    consulta = Consumer({'bootstrap.servers': BOOTSTRAP_SERVERS, 'group.id': grupo, 'enable.auto.commit': False})
    try:
        particoes = consulta.list_topics(topico, timeout=10).topics[topico].partitions
        tps = [TopicPartition(topico, p) for p in sorted(particoes)]
        commitados = consulta.committed(tps, timeout=10)
        total_commitado, lag_total, lag_por_particao = 0, 0, []
        for tp in commitados:
            _, fim = consulta.get_watermark_offsets(tp, timeout=10)
            offset = tp.offset if tp.offset >= 0 else fim  # ainda sem commit: considera em dia
            total_commitado += offset
            lag_total += fim - offset
            lag_por_particao.append(fim - offset)
        return total_commitado, lag_total, lag_por_particao
    finally:
        consulta.close()


def main():
    intervalo = float(sys.argv[1]) if len(sys.argv) > 1 else 5.0
    grupos = {g: GRUPOS[g] for g in sys.argv[2:]} if len(sys.argv) > 2 else GRUPOS

    anteriores = {}
    print(f"Medindo a cada {intervalo:.0f}s (o auto commit dos consumidores é a cada 5 s). Ctrl+C para sair.\n")
    try:
        while True:
            instante = time.time()
            print(time.strftime("%H:%M:%S"))
            print(f"  {'grupo':<38}{'lag':>10}{'consumo msg/s':>15}   lag por partição")
            for grupo, topico in grupos.items():
                commitado, lag, por_particao = medir_grupo(grupo, topico)
                if grupo in anteriores:
                    c_ant, t_ant = anteriores[grupo]
                    vazao = f"{(commitado - c_ant) / (instante - t_ant):.0f}"
                else:
                    vazao = "-"
                anteriores[grupo] = (commitado, instante)
                print(f"  {grupo:<38}{lag:>10}{vazao:>15}   {por_particao}")
            print()
            time.sleep(max(0.0, intervalo - (time.time() - instante)))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
