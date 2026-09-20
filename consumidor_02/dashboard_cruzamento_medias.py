import json
import os
import sys
import subprocess
import time
from datetime import datetime
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Dashboard - Cruzamento de Médias (Situação 2)",
    page_icon="✨",
    layout="wide"
)

st.title("✨ Monitoramento de Cruzamento de Médias (Golden Cross)")
st.caption("Detecção de tendência de alta baseada no cruzamento das Média Rápida (10s) e Lenta (60s)")

ARQUIVO_CRUZAMENTO = 'cruzamento_medias.json'


def carregar_dados():
    if os.path.exists(ARQUIVO_CRUZAMENTO):
        try:
            with open(ARQUIVO_CRUZAMENTO, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    return []


dados = carregar_dados()

if dados:
    col1, col2, col3 = st.columns(3)
    col1.metric("✨ Total de Cruzamentos (Golden Cross)", len(dados))

    ultimo = dados[0]
    ts_val = ultimo.get("timestamp", 0)
    horario_ultimo = datetime.fromtimestamp(ts_val).strftime('%H:%M:%S') if isinstance(ts_val, (int, float)) else str(ts_val)

    col2.metric("⏱️ Último Sinal Detectado", f"{ultimo.get('moeda')} ({horario_ultimo})")
    col3.metric("📈 Distanciamento (Δ%)", f"+{ultimo.get('diferenca_pct'):.4f}%")

    st.divider()

    dados_formatados = []
    for item in dados:
        ts = item.get("timestamp", 0)
        horario = datetime.fromtimestamp(ts).strftime('%H:%M:%S - %d/%m/%Y') if isinstance(ts, (int, float)) else str(ts)

        dados_formatados.append({
            "Horário": horario,
            "Moeda": item.get("moeda"),
            "Média Rápida (10s)": f"${item.get('media_rapida', 0):,.4f}",
            "Média Lenta (60s)": f"${item.get('media_lenta', 0):,.4f}",
            "Preço Atual": f"${item.get('preco_atual', 0):,.4f}",
            "Diferença Absoluta": f"+${item.get('diferenca_abs', 0):,.4f}",
            "Diferença %": f"+{item.get('diferenca_pct', 0):.4f}%"
        })

    df = pd.DataFrame(dados_formatados)
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("🎧 Aguardando sinais de cruzamento de média... Certifique-se de que o script `consumidor_cruzamento_medias.py` está rodando.")

time.sleep(2)
st.rerun()

if __name__ == "__main__":
    if not st.runtime.exists():
        subprocess.run([sys.executable, "-m", "streamlit", "run", __file__])