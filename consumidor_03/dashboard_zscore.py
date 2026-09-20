import json
import os
import sys
import subprocess
import time
from datetime import datetime
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Dashboard - Anomalia Z-Score (Situação 3)",
    page_icon="📐",
    layout="wide"
)

st.title("📐 Monitoramento de Anomalias Estatísticas (Z-Score)")
st.caption("Detecção de Outliers e Padrões de Pump/Dump usando Média (μ) e Desvio Padrão (σ)")

ARQUIVO_ANOMALIAS = 'anomalias_zscore.json'


def carregar_dados():
    if os.path.exists(ARQUIVO_ANOMALIAS):
        try:
            with open(ARQUIVO_ANOMALIAS, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    return []


dados = carregar_dados()

if dados:
    col1, col2, col3 = st.columns(3)
    col1.metric("🚨 Total de Anomalias Detectadas", len(dados))

    ultimo = dados[0]
    ts_val = ultimo.get("timestamp", 0)
    horario_ultimo = datetime.fromtimestamp(ts_val).strftime('%H:%M:%S') if isinstance(ts_val, (int, float)) else str(ts_val)

    col2.metric("⏱️ Último Outlier Registrado", f"{ultimo.get('moeda')} ({horario_ultimo})")
    col3.metric("📈 Z-Score do Último", f"{ultimo.get('z_score'):+.2f}")

    st.divider()

    dados_formatados = []
    for item in dados:
        ts = item.get("timestamp", 0)
        horario = datetime.fromtimestamp(ts).strftime('%H:%M:%S - %d/%m/%Y') if isinstance(ts, (int, float)) else str(ts)

        dados_formatados.append({
            "Horário": horario,
            "Moeda": item.get("moeda"),
            "Classificação": item.get("classificacao"),
            "Z-Score": f"{item.get('z_score'):+.4f}",
            "Preço Atual": f"${item.get('preco_atual', 0):,.4f}",
            "Média (μ)": f"${item.get('media', 0):,.4f}",
            "Desvio Padrão (σ)": f"${item.get('desvio_padrao', 0):,.4f}",
            "Amostras": item.get("amostras")
        })

    df = pd.DataFrame(dados_formatados)
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("🎧 Aguardando anomalias estatísticas... Certifique-se de que o script `consumidor_zscore.py` está rodando.")

time.sleep(2)
st.rerun()

if __name__ == "__main__":
    if not st.runtime.exists():
        subprocess.run([sys.executable, "-m", "streamlit", "run", __file__])