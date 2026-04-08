-- sql/views.sql

-- KPI 1: Resumen ejecutivo general
CREATE OR REPLACE VIEW vw_executive_summary AS
SELECT
    COUNT(*)                                          AS total_transactions,
    ROUND(SUM(amount), 2)                             AS total_volume,
    SUM(is_fraud::int)                                AS total_fraud_cases,
    ROUND(SUM(is_fraud::int)::numeric / COUNT(*) * 100, 4)
                                                      AS fraud_rate_pct,
    SUM(is_flagged_fraud::int)                        AS total_flagged,
    -- Recall: de todos los fraudes reales, cuántos fueron detectados
    ROUND(
        SUM((is_fraud AND is_flagged_fraud)::int)::numeric
        / NULLIF(SUM(is_fraud::int), 0) * 100, 2
    )                                                 AS detection_recall_pct,
    -- High risk: transacciones clasificadas como riesgo alto
    SUM((risk_tier = 'HIGH')::int)                    AS high_risk_count,
    COUNT(DISTINCT name_orig)                         AS unique_senders,
    ROUND(AVG(amount), 2)                             AS avg_transaction_amount
FROM transactions;

-- KPI 2: Fraude y volumen por tipo de transacción
CREATE OR REPLACE VIEW vw_fraud_by_type AS
SELECT
    type                                              AS transaction_type,
    COUNT(*)                                          AS total_count,
    ROUND(SUM(amount), 2)                             AS total_volume,
    SUM(is_fraud::int)                                AS fraud_count,
    ROUND(SUM(is_fraud::int)::numeric / COUNT(*) * 100, 4)
                                                      AS fraud_rate_pct,
    ROUND(AVG(amount), 2)                             AS avg_amount,
    ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER () * 100, 2)
                                                      AS pct_of_total_txns
FROM transactions
GROUP BY type
ORDER BY total_volume DESC;

-- KPI 3: Tendencia de volumen por período (step = 1 hora, agrupamos por día simulado)
CREATE OR REPLACE VIEW vw_volume_trend AS
SELECT
    (step / 24) + 1          AS simulated_day,
    COUNT(*)                 AS daily_transactions,
    ROUND(SUM(amount), 2)    AS daily_volume,
    SUM(is_fraud::int)       AS daily_fraud,
    COUNT(DISTINCT name_orig) AS active_customers
FROM transactions
GROUP BY step / 24
ORDER BY simulated_day;

-- KPI 4: Top 20 cuentas con mayor actividad sospechosa
CREATE OR REPLACE VIEW vw_suspicious_accounts AS
SELECT
    name_orig                                          AS account_id,
    COUNT(*)                                          AS total_transactions,
    SUM(is_fraud::int)                                AS fraud_count,
    ROUND(SUM(is_fraud::int)::numeric / COUNT(*) * 100, 2)
                                                      AS personal_fraud_rate_pct,
    ROUND(SUM(amount), 2)                             AS total_volume,
    ROUND(AVG(amount), 2)                             AS avg_amount,
    MAX(CASE WHEN is_fraud THEN amount END)           AS max_fraud_amount,
    COUNT(*) FILTER (WHERE type = 'TRANSFER')         AS transfer_count,
    COUNT(*) FILTER (WHERE type = 'CASH_OUT')         AS cash_out_count
FROM transactions
WHERE is_fraud = TRUE
GROUP BY name_orig
ORDER BY fraud_count DESC, total_volume DESC
LIMIT 20;

-- KPI 5: Distribución por nivel de riesgo
CREATE OR REPLACE VIEW vw_risk_distribution AS
SELECT
    risk_tier,
    COUNT(*)                                          AS count,
    ROUND(SUM(amount), 2)                             AS total_volume,
    SUM(is_fraud::int)                                AS fraud_in_tier,
    ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER () * 100, 2)
                                                      AS pct_of_total
FROM transactions
WHERE risk_tier IS NOT NULL
GROUP BY risk_tier
ORDER BY CASE risk_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END;

-- KPI 6: Análisis de balance drenado (patrón de fraude)
CREATE OR REPLACE VIEW vw_balance_drain_analysis AS
SELECT
    type,
    COUNT(*) FILTER (WHERE balance_orig_before > 0 AND balance_orig_after = 0)
                                                      AS balance_drain_count,
    COUNT(*)                                          AS total_count,
    ROUND(
        COUNT(*) FILTER (WHERE balance_orig_before > 0 AND balance_orig_after = 0)
        ::numeric / NULLIF(COUNT(*), 0) * 100, 2
    )                                                 AS drain_rate_pct,
    SUM(is_fraud::int) FILTER (WHERE balance_orig_before > 0 AND balance_orig_after = 0)
                                                      AS confirmed_fraud_drains
FROM transactions
GROUP BY type
ORDER BY drain_rate_pct DESC;