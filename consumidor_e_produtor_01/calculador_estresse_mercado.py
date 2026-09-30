import logging
from collections import deque, defaultdict

logger = logging.getLogger("CalculadorEstresse")


class CalculadorEstresseMercado:
    """
    Classe desacoplada de infraestrutura.
    Gerencia o histórico de preços em memória e executa as regras de detecção
    de Spikes e Estresse de Mercado.
    """

    def __init__(
        self,
        janela_spike_individual: float = 30.0,
        limiar_spike_pct: float = 0.05,
        janela_estresse_mercado: float = 120.0,
        min_moedas_estresse: int = 2,
        cooldown_alerta_segundos: float = 30.0
    ):
        self.janela_spike_individual = janela_spike_individual
        self.limiar_spike_pct = limiar_spike_pct
        self.janela_estresse_mercado = janela_estresse_mercado
        self.min_moedas_estresse = min_moedas_estresse
        self.cooldown_alerta_segundos = cooldown_alerta_segundos

        # Histórico de preços por moeda: { "BTCUSDT": deque([ (timestamp, preco) ]) }
        self.janelas_precos = defaultdict(deque)

        # Histórico de timestamps de spikes: { "BTCUSDT": deque([ timestamp_spike ]) }
        self.historico_spikes = defaultdict(deque)

        # Controle de cooldown do alerta
        self.ultimo_alerta_estresse = 0.0

    def processar_cotacao(self, payload: dict) -> dict | None:
        """
        Recebe a cotação e calcula a variação.
        Retorna o dicionário com o payload de alerta se houver estresse inferido,
        ou None caso contrário.
        """
        if not isinstance(payload, dict) or payload.get("tipo_evento") != "PRECO_TICKER":
            return None

        moeda = payload.get("moeda")
        preco_atual = payload.get("preco")
        ts_atual = payload.get("timestamp")

        if not moeda or preco_atual is None or ts_atual is None:
            return None

        # ETAPA 1: DETECÇÃO DE SPIKE INDIVIDUAL DE VOLATILIDADE
        historico_p = self.janelas_precos[moeda]
        historico_p.append((ts_atual, preco_atual))

        # Purga preços anteriores à janela de spike (30s)
        limite_inferior_p = ts_atual - self.janela_spike_individual
        while historico_p and historico_p[0][0] < limite_inferior_p:
            historico_p.popleft()

        if len(historico_p) >= 2:
            preco_inicial = historico_p[0][1]
            variacao_pct = ((preco_atual - preco_inicial) / preco_inicial) * 100.0

            if abs(variacao_pct) >= self.limiar_spike_pct:
                self.historico_spikes[moeda].append(ts_atual)
                logger.warning(
                    f"⚡ Spike detectado em {moeda}: {variacao_pct:+.2f}% em {self.janela_spike_individual}s!"
                )

        # ETAPA 2: CORRELAÇÃO DE MERCADO / INFERÊNCIA DO EVENTO COMPOSTO
        limite_inferior_estresse = ts_atual - self.janela_estresse_mercado
        moedas_em_estresse = []

        for m, dq_spikes in list(self.historico_spikes.items()):
            # Purga spikes mais antigos que 2 minutos (120s)
            while dq_spikes and dq_spikes[0] < limite_inferior_estresse:
                dq_spikes.popleft()

            if len(dq_spikes) > 0:
                moedas_em_estresse.append(m)

        qtd_moedas_afetadas = len(moedas_em_estresse)

        # ETAPA 3: REGRA DE DISPARO DE ALERTA (COM COOLDOWN)
        if qtd_moedas_afetadas >= self.min_moedas_estresse:
            if (ts_atual - self.ultimo_alerta_estresse) >= self.cooldown_alerta_segundos:
                self.ultimo_alerta_estresse = ts_atual

                return {
                    "tipo_evento": "ALERTA_ESTRESSE_DE_MERCADO",
                    "nivel_severidade": "CRITICO",
                    "timestamp": ts_atual,
                    "janela_analise_segundos": self.janela_estresse_mercado,
                    "qtd_moedas_afetadas": qtd_moedas_afetadas,
                    "moedas_afetadas": moedas_em_estresse,
                    "descricao": f"Estresse detectado: {qtd_moedas_afetadas} moedas apresentaram spikes de volatilidade nos últimos {int(self.janela_estresse_mercado // 60)} minutos."
                }

        return None