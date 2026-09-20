import time
import logging
import requests
from typing import Optional, Dict, List, Any, Generator, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] BGPCollector: %(message)s")
logger = logging.getLogger("BGPCollector")


class BGPCollector:
    """
    Módulo especializado na coleta e normalização de rotas BGP via RIPE Stat API.
    """
    BASE_URL = "https://stat.ripe.net/data/bgp-state/data.json"

    def __init__(
        self,
        timeout: float | Tuple[float, float] = (3.0, 20.0),
        delay_entre_consultas: float = 0.5
    ):
        self.timeout = timeout
        self.delay = delay_entre_consultas

    def sanitizar_asn(self, asn: str | int) -> str:
        """Garante que o ASN contenha apenas os dígitos numéricos (ex: 'ASN16509' -> '16509')."""
        raw = str(asn).strip().upper()
        return raw.replace("ASN", "").replace("AS", "")

    def consultar_asn(self, asn: str | int) -> Optional[Dict[str, Any]]:
        """
        Consulta a API HTTP e retorna apenas a chave do ASN e a lista de rotas normalizadas.
        Retorna None em caso de erro/timeout.
        """
        num_asn = self.sanitizar_asn(asn)
        formatted_asn = f"ASN{num_asn}"
        params = {"resource": num_asn}

        try:
            resposta = requests.get(self.BASE_URL, params=params, timeout=self.timeout)

            if resposta.status_code == 429:
                logger.warning(f"⚠️ Rate limit (429) atingido no {formatted_asn}. Pausando 10s...")
                time.sleep(10.0)
                return None

            if resposta.status_code == 200:
                data = resposta.json().get("data", {})
                bgp_state = data.get("bgp_state", [])

                # Normalização simplificada: apenas extração dos dados puros
                rotas_normalizadas = [
                    {
                        "prefixo": item.get("target_prefix"),
                        "as_path": item.get("path", [])
                    }
                    for item in bgp_state
                ]

                logger.info(f"✅ {formatted_asn} consultado | Total de rotas obtidas: {len(rotas_normalizadas)}")

                # Retorna os dados puros sem o envelope do Kafka
                return {
                    "key": formatted_asn,
                    "rotas": rotas_normalizadas
                }

            logger.error(f"❌ Erro HTTP {resposta.status_code} ao consultar {formatted_asn}")

        except requests.RequestException as err:
            logger.error(f"❌ Falha de rede/timeout ao consultar {formatted_asn}: {err}")

        return None

    def coletar_fluxo_asn(self, asn: str | int, tamanho_lote: int = 50) -> Generator[Dict[str, Any], None, None]:
        """
        Consulta um único ASN e constrói as mensagens envelopadas para o Kafka fatiadas em lotes.
        """
        logger.info(f"🔍 Coletando fluxo para o ASN: {asn}...")
        inicio = time.time()

        resultado = self.consultar_asn(asn)

        if resultado:
            asn_key = resultado["key"]
            rotas = resultado["rotas"]
            total_rotas = len(rotas)

            # Caso não haja rotas, envia um lote único com lista vazia
            if total_rotas == 0:
                yield {
                    "key": asn_key,
                    "payload": {
                        "tipo_evento": "BGP_ROUTE_CHANGE",
                        "asn": asn_key,
                        "lote_atual": 1,
                        "total_lotes": 1,
                        "total_rotas_asn": 0,
                        "total_rotas_lote": 0,
                        "dados_bgp": []
                    }
                }
            else:
                total_lotes = (total_rotas + tamanho_lote - 1) // tamanho_lote

                # Fatia a lista de rotas em blocos
                for idx, i in enumerate(range(0, total_rotas, tamanho_lote), start=1):
                    lote_rotas = rotas[i: i + tamanho_lote]

                    yield {
                        "key": asn_key,
                        "payload": {
                            "tipo_evento": "BGP_ROUTE_CHANGE",
                            "asn": asn_key,
                            "lote_atual": idx,
                            "total_lotes": total_lotes,
                            "total_rotas_asn": total_rotas,
                            "total_rotas_lote": len(lote_rotas),
                            "dados_bgp": lote_rotas
                        }
                    }

        duracao = round(time.time() - inicio, 2)
        logger.info(f"✅ ASN {asn} processado em {duracao}s.")


