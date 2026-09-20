import math
import sys
import logging
from collections import defaultdict, deque

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] ZScoreMonitor: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("ZScoreMonitor")


class MonitorAnomaliaZScore:
    """
    Mantém a janela móvel de cotações em memória RAM, calcula a Média (μ)
    e o Desvio Padrão (σ) para identificar outliers via Z-Score.
    """

    def __init__(self, janela_segundos: float = 60.0, limiar_z: float = 2.5, min_amostras: int = 5):
        self.janela_segundos = janela_segundos
        self.limiar_z = limiar_z
        self.min_amostras = min_amostras
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

        # 2. Purga cotações mais antigas que a janela móvel (60s)
        limite_inferior = ts_atual - self.janela_segundos
        while historico and historico[0][0] < limite_inferior:
            historico.popleft()

        # 3. Garante uma quantidade mínima de cotações
        if len(historico) < self.min_amostras:
            return None

        precos = [p for _, p in historico]
        n = len(precos)

        # 4. Cálculo da Média Aritmética (μ)
        media = sum(precos) / n

        # 5. Cálculo do Desvio Padrão Amostral (σ)
        variancia = sum((p - media) ** 2 for p in precos) / (n - 1)
        desvio_padrao = math.sqrt(variancia)

        if desvio_padrao == 0:
            z_score = 0.0
        else:
            z_score = (preco_atual - media) / desvio_padrao

        # 6. VERIFICAÇÃO DE ANOMALIA ESTATÍSTICA (|Z| >= limiar_z)
        if abs(z_score) >= self.limiar_z:
            classificacao = "🚀 PUMP ANÔMALO" if z_score > 0 else "💥 DUMP ANÔMALO"

            logger.warning(
                f"\n🚨 [ANOMALIA DETECTADA] {moeda} | Z-Score: {z_score:+.4f} | Preço: ${preco_atual:,.4f}"
            )

            return {
                "tipo_evento": "ANOMALIA_ZSCORE",
                "moeda": moeda,
                "classificacao": classificacao,
                "z_score": round(z_score, 4),
                "preco_atual": round(preco_atual, 4),
                "media": round(media, 4),
                "desvio_padrao": round(desvio_padrao, 4),
                "amostras": n,
                "timestamp": ts_atual
            }

        return None