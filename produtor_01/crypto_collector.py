import sys
import time
import logging
import json
import requests
from typing import Optional, Dict, List, Any, Generator, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] CryptoCollector: %(message)s",
    stream=sys.stdout  # Força o logging a usar o mesmo canal do print()
)
logger = logging.getLogger("CryptoCollector")


class CryptoCollector:
    """
    Módulo especializado na coleta e normalização de cotações de criptomoedas via API REST da Binance.
    """
    BASE_URL = "https://api.binance.com/api/v3/ticker/price"

    def __init__(
        self,
        timeout: float | Tuple[float, float] = (3.0, 10.0),
        delay_entre_consultas: float = 0.2
    ):
        self.timeout = timeout
        self.delay = delay_entre_consultas

    def sanitizar_simbolo(self, simbolo: str) -> str:
        """Garante que o símbolo esteja em maiúsculas e sem espaços (ex: 'btcusdt' -> 'BTCUSDT')."""
        return str(simbolo).strip().upper()

    def consultar_simbolo(self, simbolo: str) -> Optional[Dict[str, Any]]:
        """
        Consulta a API REST da Binance para um par específico e retorna os dados normalizados.
        Retorna None em caso de erro ou timeout.
        """
        simbolo_normalizado = self.sanitizar_simbolo(simbolo)
        params = {"symbol": simbolo_normalizado}

        try:
            resposta = requests.get(self.BASE_URL, params=params, timeout=self.timeout)

            if resposta.status_code == 429:
                logger.warning(f"⚠️ Rate limit (429) atingido na Binance para {simbolo_normalizado}. Pausando 10s...")
                time.sleep(10.0)
                return None

            if resposta.status_code == 200:
                dados = resposta.json()
                preco = float(dados.get("price", 0.0))

                logger.info(f"✅ {simbolo_normalizado} consultado | Preço: ${preco:,.2f}")

                return {
                    "key": simbolo_normalizado,
                    "simbolo": simbolo_normalizado,
                    "preco": preco,
                    "timestamp": time.time()
                }

            logger.error(f"❌ Erro HTTP {resposta.status_code} ao consultar {simbolo_normalizado}")

        except requests.RequestException as err:
            logger.error(f"❌ Falha de rede/timeout ao consultar {simbolo_normalizado}: {err}")

        return None

    def coletar_fluxo_cripto(
        self,
        simbolos: List[str]
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Consulta uma lista de pares de criptomoedas e gera as mensagens individualmente
        para o Kafka sem divisão por lotes.
        """
        logger.info(f"🔍 Coletando fluxo de cotações para {len(simbolos)} símbolo(s)...")
        inicio = time.time()

        coletados = 0
        for sim in simbolos:
            resultado = self.consultar_simbolo(sim)
            if resultado:
                coletados += 1
                yield {
                    "key": resultado["key"],
                    "payload": {
                        "tipo_evento": "PRECO_TICKER",
                        "moeda": resultado["simbolo"],
                        "preco": resultado["preco"],
                        "timestamp": resultado["timestamp"]
                    }
                }
            time.sleep(self.delay)

        duracao = round(time.time() - inicio, 2)
        logger.info(f"✅ Coleta concluída: {coletados}/{len(simbolos)} cotações obtidas em {duracao}s.\n")


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 TESTANDO COLETOR DE CRIPTOMOEDAS (SEM LOTES)")
    print("=" * 60 + "\n")

    collector = CryptoCollector(delay_entre_consultas=0.01)
    simbolos_para_testar = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]

    # Coleta os eventos e imprime cada mensagem gerada para o Kafka
    for evento_kafka in collector.coletar_fluxo_cripto(simbolos_para_testar):
        print("📦 [MENSAGEM KAFKA GERADA]")
        print(f"   ├─ Chave (Key): {evento_kafka['key']}")
        print(f"   └─ Payload JSON: {json.dumps(evento_kafka['payload'], indent=6)}")
        print("-" * 60)