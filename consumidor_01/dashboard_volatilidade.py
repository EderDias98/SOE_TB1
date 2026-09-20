import json
import os
import sys
import subprocess
import time
from datetime import datetime
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Dashboard - Volatilidade Curta (Situação 1)",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Monitoramento de Volatilidade Curta (Spikes)")
st.caption("Acompanhamento das oscilações de preços individuais em tempo real")

ARQUIVO_SPIKES = 'spikes_volatilidade.json'


def carregar_spikes():
    if os.path.exists(ARQUIVO_SPIKES):
        try:
            with open(ARQUIVO_SPIKES, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    return []


spikes = carregar_spikes()

if spikes:
    # Cartões de Métricas no Topo
    col1, col2, col3 = st.columns(3)
    col1.metric("⚡ Total de Spikes Detectados", len(spikes))

    ultimo = spikes[0]
    ts_val = ultimo.get("timestamp", 0)
    horario_ultimo = datetime.fromtimestamp(ts_val).strftime('%H:%M:%S') if isinstance(ts_val, (int, float)) else str(
        ts_val)

    col2.metric("⏱️ Último Spike Registrado", f"{ultimo.get('moeda')} ({horario_ultimo})")
    col3.metric("📈 Variação do Último Spike", f"{ultimo.get('variacao_pct'):+.2f}%")

    st.divider()

    # Formatação dos dados para a tabela
    dados_formatados = []
    for s in spikes:
        ts = s.get("timestamp", 0)
        horario = datetime.fromtimestamp(ts).strftime('%H:%M:%S - %d/%m/%Y') if isinstance(ts, (int, float)) else str(
            ts)
        p_in = s.get("preco_inicial", 0.0)
        p_at = s.get("preco_atual", 0.0)

        dados_formatados.append({
            "Horário": horario,
            "Moeda": s.get("moeda"),
            "Classificação": s.get("direcao"),
            "Variação ΔP%": f"{s.get('variacao_pct'):+.4f}%",
            "Preço Inicial": f"${p_in:,.4f}",
            "Preço Atual": f"${p_at:,.4f}",
            "Janela": f"{s.get('janela_segundos')}s"
        })

    df = pd.DataFrame(dados_formatados)
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info(
        "🎧 Aguardando oscilações de preço... Certifique-se de que o script `consumidor_volatilidade_3.py` está rodando.")

# Atualização automática da página a cada 2 segundos
time.sleep(2)
st.rerun()

if __name__ == "__main__":
    # Garante que só dispara o processo do Streamlit se o script NÃO estiver dentro do runtime
    if not st.runtime.exists():
        subprocess.run([sys.executable, "-m", "streamlit", "run", __file__])