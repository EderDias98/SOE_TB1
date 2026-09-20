import sys
import logging
from collections import defaultdict, deque
import subprocess
import streamlit as st

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] VolatilityMonitor: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("VolatilityMonitor")


class MonitorVolatilidadeCurta:
    """
    Mantém a janela móvel de cotações em memória RAM e aplica
    a verificação da Situação 1.
    """
    def __init__(self, janela_segundos: float = 60.0, limiar_pct: float = 0.05):
        self.janela_segundos = janela_segundos
        self.limiar_pct = limiar_pct
        self.janelas_precos = defaultdict(deque)

    def processar_evento(self, payload: dict):
        if not isinstance(payload, dict) or payload.get("tipo_evento") != "PRECO_TICKER":
            return None

        moeda = payload.get("moeda")
        preco_atual = payload.get("preco")
        ts_atual = payload.get("timestamp")

        if not moeda or preco_atual is None or ts_atual is None:
            return None

        historico = self.janelas_precos[moeda]

        # 1. Adiciona a cotação recebida ao histórico
        historico.append((ts_atual, preco_atual))

        # 2. Purga cotações fora da janela de tempo móvel
        limite_inferior = ts_atual - self.janela_segundos
        while historico and historico[0][0] < limite_inferior:
            historico.popleft()

        # 3. Calcula a variação
        if len(historico) >= 2:
            ts_inicial, preco_inicial = historico[0]
            variacao_pct = ((preco_atual - preco_inicial) / preco_inicial) * 100.0

            # Dispara alerta se |ΔP%| >= limiar_pct
            if abs(variacao_pct) >= self.limiar_pct:
                direcao = "📈 ALTA BRUSCA" if variacao_pct > 0 else "📉 QUEDA BRUSCA"
                logger.warning(f"⚡ Spike detectado em {moeda}: {variacao_pct:+.4f}% em {self.janela_segundos}s!")

                # Retorna o dicionário com os detalhes da Situação 1
                return {
                    "tipo_evento": "SPIKE_VOLATILIDADE",
                    "moeda": moeda,
                    "direcao": direcao,
                    "variacao_pct": round(variacao_pct, 4),
                    "preco_inicial": preco_inicial,
                    "preco_atual": preco_atual,
                    "timestamp": ts_atual,
                    "janela_segundos": self.janela_segundos
                }

        return None
