-- sql/plpgsql.sql

-- Función: calcula risk score para un cliente dado su historial
CREATE OR REPLACE FUNCTION get_customer_risk_score(p_account_id VARCHAR)
RETURNS TABLE(
    account_id        VARCHAR,
    risk_score        INTEGER,
    risk_level        VARCHAR,
    total_txns        BIGINT,
    fraud_txns        BIGINT,
    avg_amount        NUMERIC,
    max_amount        NUMERIC,
    has_balance_drain BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_score         INTEGER := 0;
    v_fraud_count   BIGINT;
    v_total         BIGINT;
    v_avg           NUMERIC;
    v_max           NUMERIC;
    v_has_drain     BOOLEAN;
BEGIN
    SELECT
        COUNT(*),
        SUM(is_fraud::int),
        AVG(amount),
        MAX(amount),
        BOOL_OR(balance_orig_before > 0 AND balance_orig_after = 0)
    INTO v_total, v_fraud_count, v_avg, v_max, v_has_drain
    FROM transactions
    WHERE name_orig = p_account_id;

    -- Scoring logic
    IF v_fraud_count > 0 THEN
        v_score := v_score + 50;
    END IF;

    IF v_has_drain THEN
        v_score := v_score + 30;
    END IF;

    IF v_avg > 200000 THEN
        v_score := v_score + 15;
    ELSIF v_avg > 50000 THEN
        v_score := v_score + 5;
    END IF;

    IF v_total < 3 THEN
        v_score := v_score + 10;  -- cuenta nueva = más riesgo
    END IF;

    RETURN QUERY SELECT
        p_account_id,
        LEAST(v_score, 100),
        CASE
            WHEN v_score >= 70 THEN 'HIGH'::VARCHAR
            WHEN v_score >= 40 THEN 'MEDIUM'::VARCHAR
            ELSE 'LOW'::VARCHAR
        END,
        v_total,
        v_fraud_count,
        ROUND(v_avg, 2),
        ROUND(v_max, 2),
        v_has_drain;
END;
$$;

-- Trigger de auditoría para la tabla transactions
CREATE OR REPLACE FUNCTION fn_audit_transactions()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO audit_log (table_name, operation, old_data, new_data)
    VALUES (
        TG_TABLE_NAME,
        TG_OP,
        CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD)::jsonb ELSE NULL END,
        CASE WHEN TG_OP != 'DELETE' THEN row_to_json(NEW)::jsonb ELSE NULL END
    );
    RETURN COALESCE(NEW, OLD);
END;
$$;

-- Aplicar trigger (solo para UPDATEs y DELETEs — no en INSERT masivo del ETL)
DROP TRIGGER IF EXISTS trg_audit_transactions ON transactions;
CREATE TRIGGER trg_audit_transactions
    AFTER UPDATE OR DELETE ON transactions
    FOR EACH ROW EXECUTE FUNCTION fn_audit_transactions();