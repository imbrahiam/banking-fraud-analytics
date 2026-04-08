# etl/load_data.py
"""
ETL Pipeline: PaySim CSV → PostgreSQL
- Extrae: Lee el CSV por chunks (maneja 6.3M rows sin OOM)
- Transforma: Limpia, valida, genera columnas derivadas de negocio
- Carga: Inserta en PostgreSQL con psycopg2 (fast COPY method)
"""
import os
import io
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DSN = (
    f"host={os.getenv('POSTGRES_HOST')} "
    f"port={os.getenv('POSTGRES_PORT')} "
    f"dbname={os.getenv('POSTGRES_DB')} "
    f"user={os.getenv('POSTGRES_USER')} "
    f"password={os.getenv('POSTGRES_PASSWORD')}"
)

# Ajustar esta ruta al CSV de PaySim
CSV_PATH = os.getenv("PAYSIM_CSV", "./data/paysim.csv")
CHUNK_SIZE = 200_000  # chunks más grandes = menos overhead


def classify_risk(row) -> str:
    """
    Clasifica el nivel de riesgo de una transacción basado en reglas de negocio bancario.
    HIGH: transferencias/retiros de alto monto o que drenan el balance
    MEDIUM: transacciones de monto significativo en tipos de riesgo moderado
    LOW: resto
    """
    if row["type"] in ("TRANSFER", "CASH_OUT"):
        if row["amount"] > 200_000:
            return "HIGH"
        if row["balance_orig_before"] > 0 and row["balance_orig_after"] == 0:
            return "HIGH"   # balance drenado completamente
        if row["amount"] > 50_000:
            return "MEDIUM"
    if row["amount"] > 100_000:
        return "MEDIUM"
    return "LOW"


def validate_chunk(df: pd.DataFrame, chunk_num: int) -> pd.DataFrame:
    """
    Aplica reglas de calidad de datos.
    Registra y descarta registros inválidos en lugar de fallar.
    """
    original_len = len(df)
    issues = []

    # Regla 1: Monto debe ser positivo
    invalid_amount = df[df["amount"] <= 0]
    if len(invalid_amount) > 0:
        issues.append(f"  ✗ {len(invalid_amount)} registros con monto <= 0 descartados")
        df = df[df["amount"] > 0]

    # Regla 2: Tipo debe ser válido
    valid_types = {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}
    invalid_type = df[~df["type"].isin(valid_types)]
    if len(invalid_type) > 0:
        issues.append(f"  ✗ {len(invalid_type)} registros con tipo inválido descartados")
        df = df[df["type"].isin(valid_types)]

    # Regla 3: Cuentas origen/destino no nulas
    nulls = df[df["nameOrig"].isna() | df["nameDest"].isna()]
    if len(nulls) > 0:
        issues.append(f"  ✗ {len(nulls)} registros con cuentas nulas descartados")
        df = df[df["nameOrig"].notna() & df["nameDest"].notna()]

    if issues:
        print(f"  Chunk {chunk_num} — calidad de datos:")
        for issue in issues:
            print(issue)

    passed = len(df)
    if passed < original_len:
        print(f"  {original_len - passed} de {original_len} registros descartados ({(original_len-passed)/original_len*100:.2f}%)")

    return df


def transform_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el chunk y agrega columnas derivadas de negocio.
    """
    df = df.rename(columns={
        "nameOrig":        "name_orig",
        "oldbalanceOrg":   "balance_orig_before",
        "newbalanceOrig":  "balance_orig_after",
        "nameDest":        "name_dest",
        "oldbalanceDest":  "balance_dest_before",
        "newbalanceDest":  "balance_dest_after",
        "isFraud":         "is_fraud",
        "isFlaggedFraud":  "is_flagged_fraud",
    })

    # Columnas derivadas de negocio
    df["balance_delta"] = df["balance_orig_before"] - df["balance_orig_after"]
    df["risk_tier"] = df.apply(classify_risk, axis=1)

    # Booleans correctos
    df["is_fraud"] = df["is_fraud"].astype(bool)
    df["is_flagged_fraud"] = df["is_flagged_fraud"].astype(bool)

    return df[[
        "step", "type", "amount",
        "name_orig", "balance_orig_before", "balance_orig_after",
        "name_dest", "balance_dest_before", "balance_dest_after",
        "is_fraud", "is_flagged_fraud",
        "balance_delta", "risk_tier",
    ]]


def copy_chunk(conn, df: pd.DataFrame) -> None:
    """
    Usa PostgreSQL COPY para carga masiva — 10-50x más rápido que INSERT.
    Escribe el DataFrame a un buffer en memoria y lo envía por COPY FROM STDIN.
    """
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    columns = ", ".join(df.columns)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY transactions ({columns}) FROM STDIN WITH (FORMAT CSV, NULL '')",
            buffer,
        )


def run_etl():
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    total = 0

    print(f"Iniciando ETL: {CSV_PATH}")
    print(f"Chunk size: {CHUNK_SIZE:,} rows\n")

    try:
        for i, chunk in enumerate(pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE)):
            chunk = validate_chunk(chunk, i + 1)
            chunk = transform_chunk(chunk)
            copy_chunk(conn, chunk)
            conn.commit()

            total += len(chunk)
            print(f"Chunk {i+1}: {len(chunk):,} rows | Total: {total:,}")

        print(f"\n✓ ETL completado: {total:,} registros cargados")

        # Verificación post-carga
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), SUM(is_fraud::int) FROM transactions")
            count, fraud = cur.fetchone()
            print(f"✓ Verificación DB: {count:,} rows, {fraud:,} casos de fraude")

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


if __name__ == "__main__":
    run_etl()