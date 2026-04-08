-- sql/schema.sql

-- Tabla principal de transacciones
CREATE TABLE IF NOT EXISTS transactions (
    id               BIGSERIAL PRIMARY KEY,
    step             INTEGER NOT NULL,
    type             VARCHAR(20) NOT NULL,
    amount           NUMERIC(18, 2) NOT NULL,
    name_orig        VARCHAR(50) NOT NULL,
    balance_orig_before NUMERIC(18, 2),
    balance_orig_after  NUMERIC(18, 2),
    name_dest        VARCHAR(50) NOT NULL,
    balance_dest_before NUMERIC(18, 2),
    balance_dest_after  NUMERIC(18, 2),
    is_fraud         BOOLEAN NOT NULL DEFAULT FALSE,
    is_flagged_fraud BOOLEAN NOT NULL DEFAULT FALSE,
    -- Columnas derivadas calculadas en ETL
    balance_delta    NUMERIC(18, 2),   -- cuánto cambió el balance del origen
    risk_tier        VARCHAR(10),      -- HIGH / MEDIUM / LOW
    loaded_at        TIMESTAMP DEFAULT NOW()
);

-- Tabla de auditoría inmutable
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id    BIGSERIAL PRIMARY KEY,
    table_name  VARCHAR(50),
    operation   VARCHAR(10),
    old_data    JSONB,
    new_data    JSONB,
    changed_at  TIMESTAMP DEFAULT NOW(),
    changed_by  VARCHAR(100) DEFAULT current_user
);

-- Índices para performance en las queries del dashboard
CREATE INDEX IF NOT EXISTS idx_txn_type     ON transactions(type);
CREATE INDEX IF NOT EXISTS idx_txn_fraud    ON transactions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_txn_step     ON transactions(step);
CREATE INDEX IF NOT EXISTS idx_txn_name_orig ON transactions(name_orig);
CREATE INDEX IF NOT EXISTS idx_txn_risk_tier ON transactions(risk_tier);