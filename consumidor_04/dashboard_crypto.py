import json
import os
import time
from datetime import datetime
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Dashboard Crypto", page_icon="🚨", layout="wide")

st.title("🚨 Monitoramento de Estresse de Mercado")
st.caption("Visualização simples dos eventos derivados consumidos do Kafka")

ARQUIVO_ALERTAS = 'alertas.json'


def carregar_alertas():
    if os.path.exists(ARQUIVO_ALERTAS):
        try:
            with open(ARQUIVO_ALERTAS, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    return []


alertas = carregar_alertas()

if alertas:
    st.metric("Total de Alertas Detectados", len(alertas))
    st.divider()

    dados_formatados = []
    for a in alertas:
        ts = a.get("timestamp", 0)
        horario = datetime.fromtimestamp(ts).strftime('%H:%M:%S - %d/%m/%Y') if isinstance(ts,
                                                                                            (int, float)) else str(ts)
        moedas = ", ".join(a.get("moedas_afetadas", [])) if isinstance(a.get("moedas_afetadas"), list) else str(
            a.get("moedas_afetadas", ""))

        dados_formatados.append({
            "Horário": horario,
            "Tipo de Evento": a.get("tipo_evento"),
            "Severidade": a.get("nivel_severidade"),
            "Qtd Moedas": a.get("qtd_moedas_afetadas"),
            "Moedas Afetadas": moedas,
            "Descrição": a.get("descricao")
        })

    df = pd.DataFrame(dados_formatados)
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("🎧 Aguardando eventos... Certifique-se de que o script `consumidor_alertas.py` está rodando.")

# Atualização automática da página a cada 3 segundos
time.sleep(3)
st.rerun()