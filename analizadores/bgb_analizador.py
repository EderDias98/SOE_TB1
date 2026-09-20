import logging
from typing import Dict, List, Any

logger = logging.getLogger("BGPAnalyzer")


class BGPAnalyzer:
    def __init__(self):
        # Memória enxuta: { "ASN28329": { "177.107.32.0/19": [3356, 28329] } }
        self.estado_rotas: Dict[str, Dict[str, List[int]]] = {}

    def _sanitizar_as_path(self, as_path_raw: Any) -> List[int]:
        if not isinstance(as_path_raw, list):
            return []
        path_sanitizado = []
        for item in as_path_raw:
            try:
                path_sanitizado.append(int(item))
            except (ValueError, TypeError):
                continue
        return path_sanitizado

    def _sanitizar_asn(self, asn_raw: Any) -> str:
        num = str(asn_raw).strip().upper().replace("ASN", "").replace("AS", "")
        return f"ASN{num}"

    def analisar_evento(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict) or payload.get("tipo_evento") != "BGP_ROUTE_CHANGE":
            return []

        asn_raw = payload.get("asn")
        dados_bgp = payload.get("dados_bgp", [])

        if not asn_raw or not isinstance(dados_bgp, list):
            return []

        asn = self._sanitizar_asn(asn_raw)

        if asn not in self.estado_rotas:
            self.estado_rotas[asn] = {}

        alertas = []

        for rota in dados_bgp:
            if not isinstance(rota, dict):
                continue

            prefixo = rota.get("prefixo")
            if not prefixo:
                continue

            as_path_full = self._sanitizar_as_path(rota.get("as_path", []))

            # ------------------------------------------------------------------
            # SOLUÇÃO DE MEMÓRIA: Slicing para remover o primeiro salto (borda RIPE)
            # Exemplo: [48237, 3356, 28329] vira [3356, 28329]
            # Exemplo: [200612, 3356, 28329] vira [3356, 28329] (IDs iguais!)
            # ------------------------------------------------------------------
            as_path_util = as_path_full[1:] if len(as_path_full) > 1 else as_path_full

            if prefixo in self.estado_rotas[asn]:
                as_path_anterior_util = self.estado_rotas[asn][prefixo]

                if as_path_anterior_util != as_path_util:
                    alerta = {
                        "tipo_alerta": "SITUACAO_INTERESSE_1_ALTERACAO_ROTA",
                        "asn": asn,
                        "prefixo": prefixo,
                        "as_path_anterior": as_path_anterior_util,
                        "as_path_novo": as_path_util
                    }
                    alertas.append(alerta)

                    logger.warning(
                        f"\n🚨 [ALTERAÇÃO DE ROTA REAL DETECTADA]\n"
                        f"   ├─ ASN:               {asn}\n"
                        f"   ├─ Prefixo Afetado:   {prefixo}\n"
                        f"   ├─ AS Path Anterior:  {as_path_anterior_util}\n"
                        f"   └─ AS Path Novo:      {as_path_util}\n"
                    )

            # Salva apenas a rota útil na memória em RAM
            self.estado_rotas[asn][prefixo] = as_path_util

        return alertas