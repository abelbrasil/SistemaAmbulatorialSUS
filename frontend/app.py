import pandas as pd
import streamlit as st
import plotly.express as px

from services.api import (
    get_cnes,
    get_procedimentos,
    get_calendario,
    get_analitico
)

st.set_page_config(layout="wide")
st.title("📊 Dashboard SIASUS")

# ==============================
# CACHE
# ==============================

@st.cache_data
def load(endpoint, params=None):
    if endpoint == "/cnes":
        return get_cnes()
    elif endpoint == "/procedimentos":
        return get_procedimentos()
    elif endpoint == "/calendario":
        return get_calendario()
    elif endpoint == "/analitico":
        return get_analitico(params)
    else:
        return pd.DataFrame()

# ==============================
# FILTROS
# ==============================

cnes_df = load("/cnes")
proc_df = load("/procedimentos")
cal_df = load("/calendario")

cnes_df["label"] = cnes_df["estabelecimento"]
proc_df["label"] = proc_df["procedimento"] + " - " + proc_df["descricao"]
proc_df = proc_df.sort_values(by="label")

# ==============================
# SIDEBAR
# ==============================

# CNES
cnes_sel = st.sidebar.multiselect(
    "Estabelecimento",
    cnes_df["label"]
)

cnes_map = dict(zip(cnes_df["label"], cnes_df["cnes"]))
cnes_values = [cnes_map[x] for x in cnes_sel]

# ------------------------------

# PROCEDIMENTO
proc_sel = st.sidebar.multiselect(
    "Procedimento",
    proc_df["label"]
)

proc_map = dict(zip(proc_df["label"], proc_df["procedimento"]))
proc_values = [proc_map[x] for x in proc_sel]

# ------------------------------

# ANO
anos = sorted(cal_df["ano"].unique())

ano_sel = st.sidebar.multiselect(
    "Ano",
    anos
)

# ==============================
# PARAMS
# ==============================

params = {}

if cnes_values:
    params["cnes"] = cnes_values

if proc_values:
    params["procedimento"] = proc_values

if ano_sel:
    params["ano"] = ano_sel

# ==============================
# DADOS
# ==============================

df = load("/analitico", params)

if df.empty:
    st.warning("Sem dados")
    st.stop()

# ==============================
# 📊 KPI CARDS (TOPO)
# ==============================

total_qtd = int(df["quantidade"].sum())
total_valor = float(df["valor"].sum())

col1, col2 = st.columns(2)

with col1:
    st.markdown(
        f"""
        <div style="
            background-color:#1f77b4;
            padding:20px;
            border-radius:10px;
            text-align:center;
            color:white;
        ">
            <h4>Quantidade Total</h4>
            <h2>{total_qtd:,}</h2>
        </div>
        """,
        unsafe_allow_html=True
    )

with col2:
    st.markdown(
        f"""
        <div style="
            background-color:#d62728;
            padding:20px;
            border-radius:10px;
            text-align:center;
            color:white;
        ">
            <h4>Valor Total</h4>
            <h2>R$ {total_valor:,.2f}</h2>
        </div>
        """,
        unsafe_allow_html=True
    )

# ==============================
# 🔥 DIMENSÃO CALENDÁRIO (API)
# ==============================

cal_map = dict(zip(cal_df["anomes"], cal_df["label"]))
df["mes_label"] = df["anomes"].map(cal_map).fillna(df["anomes"])

# ==============================
# AGREGAÇÕES
# ==============================

df_mes = (
    df.groupby(["anomes", "mes_label"], as_index=False)
    .agg({
        "quantidade": "sum",
        "valor": "sum"
    })
    .sort_values("anomes")
)

df_ano = (
    df.groupby("ano", as_index=False)
    .agg({
        "quantidade": "sum",
        "valor": "sum"
    })
)

df_cnes = (
    df.groupby("estabelecimento", as_index=False)
    .agg({
        "quantidade": "sum",
        "valor": "sum"
    })
)

df_proc = (
    df.groupby("descricao", as_index=False)
    .agg({
        "quantidade": "sum",
        "valor": "sum"
    })
)

# ==============================
# 📈 GRÁFICOS POR MÊS (LINHA)
# ==============================

col1, col2 = st.columns(2)

fig_mes_qtd = px.line(
    df_mes,
    x="mes_label",
    y="quantidade",
    markers=True,
    color_discrete_sequence=["blue"]
)

fig_mes_qtd.update_layout(xaxis=dict(type="category"))

fig_mes_val = px.line(
    df_mes,
    x="mes_label",
    y="valor",
    markers=True,
    color_discrete_sequence=["red"]
)

fig_mes_val.update_layout(xaxis=dict(type="category"))

col1.plotly_chart(fig_mes_qtd, use_container_width=True)
col2.plotly_chart(fig_mes_val, use_container_width=True)

# ==============================
# 📊 GRÁFICOS POR ANO (COLUNA)
# ==============================

col1, col2 = st.columns(2)

fig_ano_qtd = px.bar(
    df_ano,
    x="ano",
    y="quantidade",
    color_discrete_sequence=["blue"]
)

fig_ano_qtd.update_layout(xaxis=dict(type="category"))

fig_ano_val = px.bar(
    df_ano,
    x="ano",
    y="valor",
    color_discrete_sequence=["red"]
)

fig_ano_val.update_layout(xaxis=dict(type="category"))

col1.plotly_chart(fig_ano_qtd, use_container_width=True)
col2.plotly_chart(fig_ano_val, use_container_width=True)

# ==============================
# 🏥 GRÁFICOS POR ESTABELECIMENTO
# ==============================

st.subheader("🏥 Produção por Estabelecimento")

fig_cnes_qtd = px.bar(
    df_cnes,
    x="quantidade",
    y="estabelecimento",
    orientation="h",
    color_discrete_sequence=["blue"]
)

st.plotly_chart(fig_cnes_qtd, use_container_width=True)

fig_cnes_val = px.bar(
    df_cnes,
    x="valor",
    y="estabelecimento",
    orientation="h",
    color_discrete_sequence=["red"]
)

st.plotly_chart(fig_cnes_val, use_container_width=True)

# ==============================
# 📋 LISTA DE PROCEDIMENTOS
# ==============================

st.subheader("📋 Procedimentos")

st.dataframe(
    df_proc.sort_values("quantidade", ascending=False),
    use_container_width=True
)