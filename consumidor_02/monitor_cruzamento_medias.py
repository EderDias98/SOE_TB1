import sys
import logging
from collections import defaultdict, deque

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] TrendMonitor: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("TrendMonitor")


class MonitorCruzamentoMedias:
    """
    Calcula as Médias Móveis Rápida (10s) e Lenta (60s) em memória RAM
    e identifica o evento de Golden Cross (Cruzamento Ascendente).
    """

    def __init__(self, janela_rapida: float = 10.0, janela_lenta: float = 60.0):
        self.janela_rapida = janela_rapida
        self.janela_lenta = janela_lenta
        self.janelas_precos = defaultdict(deque)
        self.estados_anteriores = defaultdict(lambda: None)

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

        # 2. Purga cotações mais antigas que a janela máxima (Média Lenta = 60s)
        limite_inferior_lenta = ts_atual - self.janela_lenta
        while historico and historico[0][0] < limite_inferior_lenta:
            historico.popleft()

        if len(historico) < 2:
            return None

        # 3. Calcula Média Rápida (últimos 10s) e Média Lenta (últimos 60s)
        limite_inferior_rapida = ts_atual - self.janela_rapida
        precos_10s = [p for ts, p in historico if ts >= limite_inferior_rapida]
        precos_60s = [p for ts, p in historico]

        if not precos_10s or not precos_60s:
            return None

        media_rapida = sum(precos_10s) / len(precos_10s)
        media_lenta = sum(precos_60s) / len(precos_60s)

        estado_atual = "ACIMA" if media_rapida > media_lenta else "ABAIXO"
        estado_anterior = self.estados_anteriores[moeda]

        # 4. VERIFICAÇÃO DO GOLDEN CROSS (Cruzamento Ascendente)
        alerta_evento = None
        if estado_anterior == "ABAIXO" and estado_atual == "ACIMA":
            diferenca_abs = media_rapida - media_lenta
            diferenca_pct = (diferenca_abs / media_lenta) * 100.0

            logger.warning(
                f"\n✨ [GOLDEN CROSS DETECTADO] {moeda} | MA10s: ${media_rapida:,.4f} > MA60s: ${media_lenta:,.4f}"
            )

            alerta_evento = {
                "tipo_evento": "GOLDEN_CROSS",
                "moeda": moeda,
                "media_rapida": round(media_rapida, 4),
                "media_lenta": round(media_lenta, 4),
                "preco_atual": round(preco_atual, 4),
                "diferenca_abs": round(diferenca_abs, 4),
                "diferenca_pct": round(diferenca_pct, 4),
                "timestamp": ts_atual
            }

        # Atualiza o estado da moeda
        self.estados_anteriores[moeda] = estado_atual
        return alerta_evento