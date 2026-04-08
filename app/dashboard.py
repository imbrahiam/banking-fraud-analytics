# app/dashboard.py
"""
Banking Fraud Analytics Dashboard
KPIs: fraud rate, transaction volume, risk distribution, suspicious accounts
Fuente: PostgreSQL — vistas analíticas sobre PaySim 6.3M transactions
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Banking Fraud Analytics",
    page_icon="🏦",
    layout="wide",
)

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

# ── Data loading ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load(query: str) -> pd.DataFrame:
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


# ── Header ────────────────────────────────────────────────────────────────────
st.title("🏦 Banking Fraud Analytics Dashboard")
st.caption("Pipeline ETL → PostgreSQL → Analytics | Dataset: PaySim 6.3M transacciones financieras")

# ── KPI Cards (fila superior) ─────────────────────────────────────────────────
summary = load("SELECT * FROM vw_executive_summary").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Transacciones",  f"{int(summary['total_transactions']):,}")
c2.metric("Volumen Total",        f"${float(summary['total_volume']):,.0f}")
c3.metric("Tasa de Fraude",       f"{float(summary['fraud_rate_pct']):.4f}%")
c4.metric("Precisión de Detección", f"{float(summary['detection_precision_pct'] or 0):.1f}%")

st.divider()

# ── Row 1: Volumen por tipo + Tendencia temporal ───────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Volumen y Fraude por Tipo de Transacción")
    df_type = load("SELECT * FROM vw_fraud_by_type")
    fig = go.Figure()
    fig.add_bar(
        x=df_type["transaction_type"],
        y=df_type["total_volume"],
        name="Volumen Total",
        marker_color="#1f77b4",
    )
    fig.add_bar(
        x=df_type["transaction_type"],
        y=df_type["fraud_rate_pct"],
        name="Tasa de Fraude (%)",
        marker_color="#d62728",
        yaxis="y2",
    )
    fig.update_layout(
        yaxis=dict(title="Volumen ($)"),
        yaxis2=dict(title="Fraud Rate (%)", overlaying="y", side="right"),
        barmode="group",
        legend=dict(orientation="h"),
        height=380,
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Tendencia de Volumen Diario")
    df_trend = load("SELECT * FROM vw_volume_trend")
    fig2 = px.area(
        df_trend,
        x="simulated_day",
        y="daily_volume",
        color_discrete_sequence=["#1f77b4"],
        labels={"simulated_day": "Día Simulado", "daily_volume": "Volumen ($)"},
        height=380,
    )
    fig2.add_scatter(
        x=df_trend["simulated_day"],
        y=df_trend["daily_fraud"] * df_trend["daily_volume"].max() / df_trend["daily_fraud"].max(),
        mode="lines",
        name="Fraude (normalizado)",
        line=dict(color="#d62728", dash="dot"),
    )
    st.plotly_chart(fig2, use_container_width=True)

# ── Row 2: Risk distribution + Balance drain ─────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("Distribución por Nivel de Riesgo")
    df_risk = load("SELECT * FROM vw_risk_distribution")
    color_map = {"HIGH": "#d62728", "MEDIUM": "#ff7f0e", "LOW": "#2ca02c"}
    fig3 = px.pie(
        df_risk,
        names="risk_tier",
        values="count",
        color="risk_tier",
        color_discrete_map=color_map,
        hole=0.4,
        height=380,
    )
    fig3.update_traces(textinfo="label+percent+value")
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("Balance Drenado por Tipo (Patrón de Fraude)")
    df_drain = load("SELECT * FROM vw_balance_drain_analysis")
    fig4 = px.bar(
        df_drain,
        x="type",
        y="drain_rate_pct",
        color="confirmed_fraud_drains",
        color_continuous_scale="Reds",
        labels={
            "type": "Tipo",
            "drain_rate_pct": "% Transacciones con Balance Drenado",
            "confirmed_fraud_drains": "Fraudes Confirmados",
        },
        height=380,
    )
    st.plotly_chart(fig4, use_container_width=True)

# ── Row 3: Top cuentas sospechosas ────────────────────────────────────────────
st.subheader("Top 20 Cuentas con Mayor Actividad Fraudulenta")
df_susp = load("SELECT * FROM vw_suspicious_accounts")
df_susp_display = df_susp.rename(columns={
    "account_id": "Cuenta",
    "total_transactions": "Transacciones",
    "fraud_count": "Fraudes",
    "personal_fraud_rate_pct": "Tasa Fraude %",
    "total_volume": "Volumen Total ($)",
    "avg_amount": "Monto Promedio ($)",
    "max_fraud_amount": "Mayor Fraude ($)",
})
st.dataframe(
    df_susp_display[[
        "Cuenta", "Transacciones", "Fraudes",
        "Tasa Fraude %", "Volumen Total ($)", "Mayor Fraude ($)"
    ]],
    use_container_width=True,
    hide_index=True,
)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "📊 **BancoData Analytics** | "
    "ETL: Python + pandas + psycopg2 | "
    "DB: PostgreSQL (6.3M rows, 6 vistas KPI) | "
    "Stack: Streamlit + Plotly | "
    "github.com/imbrahiam/banking-fraud-analytics"
)