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

    TAMANHO_LOTE = 100  # máximo de símbolos por requisição em lote

    def __init__(
        self,
        timeout: float | Tuple[float, float] = (3.0, 10.0),
        delay_entre_consultas: float = 0.2,
        consulta_em_lote: bool = True
    ):
        self.timeout = timeout
        self.delay = delay_entre_consultas
        # True: uma requisição traz o preço de até 100 moedas (o tempo do ciclo quase não cresce com o número de moedas).
        # False: uma requisição por moeda, em sequência (o ciclo cresce ~0,35 s por moeda).
        self.consulta_em_lote = consulta_em_lote

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

    def consultar_lote(self, simbolos: List[str]) -> List[Dict[str, Any]]:
        """
        Consulta o preço de vários pares em uma única requisição (parâmetro 'symbols' da Binance).
        Se o lote for recusado (ex.: algum símbolo inválido), consulta um a um.
        """
        simbolos_normalizados = [self.sanitizar_simbolo(s) for s in simbolos]
        params = {"symbols": json.dumps(simbolos_normalizados, separators=(",", ":"))}

        try:
            resposta = requests.get(self.BASE_URL, params=params, timeout=self.timeout)

            if resposta.status_code == 429:
                logger.warning("⚠️ Rate limit (429) atingido na Binance. Pausando 10s...")
                time.sleep(10.0)
                return []

            if resposta.status_code == 200:
                agora = time.time()
                return [
                    {"key": d["symbol"], "simbolo": d["symbol"], "preco": float(d["price"]), "timestamp": agora}
                    for d in resposta.json()
                ]

            logger.warning(f"⚠️ Lote recusado (HTTP {resposta.status_code}): {resposta.text[:150]}. Consultando um a um...")

        except requests.RequestException as err:
            logger.error(f"❌ Falha de rede/timeout na consulta em lote: {err}")
            return []

        resultados = []
        for sim in simbolos_normalizados:
            resultado = self.consultar_simbolo(sim)
            if resultado:
                resultados.append(resultado)
            time.sleep(self.delay)
        return resultados

    def _consultar_todos(self, simbolos: List[str]) -> Generator[Dict[str, Any], None, None]:
        if self.consulta_em_lote:
            for i in range(0, len(simbolos), self.TAMANHO_LOTE):
                yield from self.consultar_lote(simbolos[i:i + self.TAMANHO_LOTE])
        else:
            for sim in simbolos:
                resultado = self.consultar_simbolo(sim)
                if resultado:
                    yield resultado
                time.sleep(self.delay)

    def coletar_fluxo_cripto(
        self,
        simbolos: List[str]
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Consulta uma lista de pares de criptomoedas e gera uma mensagem por moeda para o Kafka.
        """
        modo = "em lote" if self.consulta_em_lote else "uma a uma"
        logger.info(f"🔍 Coletando fluxo de cotações para {len(simbolos)} símbolo(s) ({modo})...")
        inicio = time.time()

        coletados = 0
        for resultado in self._consultar_todos(simbolos):
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