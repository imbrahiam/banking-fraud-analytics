# Banking Fraud Analytics Dashboard

Pipeline ETL + dashboard interactivo sobre 6.3M transacciones financieras (PaySim). Desplegado en Streamlit Cloud como URL pública de portafolio.

**Live demo:** [banking-fraud-analytics-bhd.streamlit.app](https://banking-fraud-analytics-bhd.streamlit.app)

## Stack

Python · pandas · SQLAlchemy · PostgreSQL · PL/pgSQL · Streamlit · Plotly

## Arquitectura

```
CSV (PaySim 6.3M rows)
  → ETL Python (pandas + SQLAlchemy)
    → PostgreSQL (schema normalizado + 6 KPI views + PL/pgSQL)
      → Streamlit Dashboard (4 KPI cards + 5 charts)
```

## Estructura

```
banking-fraud-analytics/
├── etl/
│   └── load_data.py        # ETL: CSV → PostgreSQL con validaciones y columnas derivadas
├── sql/
│   ├── schema.sql           # Tabla transactions + audit_log + índices
│   ├── views.sql            # 6 vistas analíticas de KPIs bancarios
│   └── plpgsql.sql          # Risk scoring function + audit trigger
├── app/
│   └── dashboard.py         # Streamlit app
└── requirements.txt
```

## KPIs y vistas SQL

| Vista | KPI |
|-------|-----|
| `vw_executive_summary` | Fraud rate, volumen total, detección |
| `vw_fraud_by_type` | Fraude y volumen por tipo de transacción |
| `vw_volume_trend` | Tendencia temporal de volumen |
| `vw_risk_distribution` | Distribución por tier de riesgo (HIGH/MEDIUM/LOW) |
| `vw_suspicious_accounts` | Top cuentas por score de riesgo |
| `vw_balance_drain_analysis` | Ratio de balance drenado por tipo |

## Setup local

```bash
# 1. Clonar e instalar
pip install -r requirements.txt

# 2. Configurar base de datos
cp .env.example .env
# Editar .env con tus credenciales PostgreSQL

# 3. Crear schema y cargar datos
psql -U postgres -d bancodata -f sql/schema.sql
psql -U postgres -d bancodata -f sql/views.sql
psql -U postgres -d bancodata -f sql/plpgsql.sql
python etl/load_data.py

# 4. Correr dashboard
streamlit run app/dashboard.py
```

## Variables de entorno

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=tu_password
POSTGRES_DB=bancodata
```

> El dataset PaySim (`paysim_dataset.csv`, 470MB) no está incluido en el repo.
> Descárgalo desde [Kaggle — PaySim Synthetic Financial Dataset](https://www.kaggle.com/datasets/ealaxi/paysim1).
