"""
Teste com muitas moedas: troca a lista do produtor (produtor_01/moedas.json) pelas N moedas
com maior volume em USDT na Binance. O produtor relê o arquivo a cada ciclo, então a troca
vale sem reiniciar nada.

Uso:
    python testes/configurar_moedas.py 50          -> usa as 50 moedas de maior volume
    python testes/configurar_moedas.py --restaurar -> volta a lista original
"""
import re
import sys
import json
import shutil
from pathlib import Path
import requests

ARQUIVO_MOEDAS = Path(__file__).resolve().parent.parent / "produtor_01" / "moedas.json"
ARQUIVO_BACKUP = ARQUIVO_MOEDAS.with_name("moedas_original.json")
URL_TICKER_24H = "https://api.binance.com/api/v3/ticker/24hr"

# Stablecoins e pares "parados" não geram variação de preço interessante
IGNORAR = {"USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "USDPUSDT", "DAIUSDT", "EURUSDT", "BUSDUSDT", "USD1USDT"}


def buscar_top_moedas(quantidade):
    resposta = requests.get(URL_TICKER_24H, timeout=(3.0, 15.0))
    resposta.raise_for_status()
    pares = [
        t for t in resposta.json()
        if t["symbol"].endswith("USDT") and t["symbol"] not in IGNORAR and float(t["lastPrice"]) > 0
        # Há moedas com nome em chinês; a consulta em lote da Binance recusa esses símbolos
        and re.fullmatch(r"[A-Z0-9]+", t["symbol"])
    ]
    pares.sort(key=lambda t: float(t["quoteVolume"]), reverse=True)
    return [t["symbol"] for t in pares[:quantidade]]


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)

    if sys.argv[1] == "--restaurar":
        if not ARQUIVO_BACKUP.exists():
            print("Nenhum backup encontrado; a lista atual já é a original.")
            return
        shutil.move(ARQUIVO_BACKUP, ARQUIVO_MOEDAS)
        print(f"Lista original restaurada em {ARQUIVO_MOEDAS}")
        return

    quantidade = int(sys.argv[1])
    if not ARQUIVO_BACKUP.exists():
        shutil.copy(ARQUIVO_MOEDAS, ARQUIVO_BACKUP)

    moedas = buscar_top_moedas(quantidade)
    with open(ARQUIVO_MOEDAS, "w", encoding="utf-8") as f:
        json.dump({"moedas": moedas}, f, indent=2)
    print(f"{len(moedas)} moedas gravadas em {ARQUIVO_MOEDAS} (backup em {ARQUIVO_BACKUP.name})")
    print(", ".join(moedas))


if __name__ == "__main__":
    main()
