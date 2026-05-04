# ============================================================
# DAG 1: BTC Historical Daily OHLCV
# Source:   Yahoo Finance (yfinance)
# Table:    USER_DB_GOPHER.RAW.BTC_DAILY
# Schedule: 00:30 UTC every day (fetches previous day's data)
# On success: automatically triggers btc_fng_daily
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta, timezone
import yfinance as yf
import pandas as pd

# ── Config ────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_DB      = "USER_DB_GOPHER"
SNOWFLAKE_SCHEMA  = "RAW"
TABLE_NAME        = "BTC_DAILY"


# ── Task 1: Auto-create table ─────────────────────────────────
def create_table_if_not_exists(**context):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()
    cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date                DATE          NOT NULL PRIMARY KEY,
            open                FLOAT,
            high                FLOAT,
            low                 FLOAT,
            close               FLOAT,
            volume              BIGINT,
            yfinance_fetched_at TIMESTAMP_NTZ,
            inserted_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    conn.commit()
    cur.close()
    print(f"[{TABLE_NAME}] ✅ Table ready")


# ── Task 2: Fetch yesterday's OHLCV and load to Snowflake ─────
def fetch_and_load_historical(**context):
    # data_interval_start = yesterday (Airflow's execution date)
    exec_date = context["data_interval_start"].date()
    start_str = exec_date.strftime("%Y-%m-%d")
    end_str   = (exec_date + timedelta(days=1)).strftime("%Y-%m-%d")
    now_utc   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    print(f"[yfinance] Fetching BTC-USD for {start_str}")

    df = yf.download(
        tickers     = "BTC-USD",
        start       = start_str,
        end         = end_str,
        interval    = "1d",
        auto_adjust = True,
        progress    = False,
    )

    if df.empty:
        raise ValueError(
            f"yfinance returned no data for {start_str}. "
            "Check if the date is a valid trading day."
        )

    df.reset_index(inplace=True)

    # Flatten multi-level columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    df.rename(columns={
        "Date":   "date",
        "Open":   "open",
        "High":   "high",
        "Low":    "low",
        "Close":  "close",
        "Volume": "volume",
    }, inplace=True)

    df["date"]   = pd.to_datetime(df["date"]).dt.date
    df["open"]   = df["open"].round(2)
    df["high"]   = df["high"].round(2)
    df["low"]    = df["low"].round(2)
    df["close"]  = df["close"].round(2)
    df["volume"] = df["volume"].astype("int64")
    df.dropna(inplace=True)

    row = df.iloc[0]

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()
    cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    # MERGE ensures idempotency — safe to re-run on failure
    cur.execute(f"""
        MERGE INTO {TABLE_NAME} AS target
        USING (SELECT %s::DATE AS date) AS source
          ON target.date = source.date
        WHEN MATCHED THEN UPDATE SET
            open                = %s,
            high                = %s,
            low                 = %s,
            close               = %s,
            volume              = %s,
            yfinance_fetched_at = %s
        WHEN NOT MATCHED THEN INSERT
            (date, open, high, low, close, volume, yfinance_fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        str(exec_date),
        round(float(row["open"]),  2),
        round(float(row["high"]),  2),
        round(float(row["low"]),   2),
        round(float(row["close"]), 2),
        int(row["volume"]),
        now_utc,
        str(exec_date),
        round(float(row["open"]),  2),
        round(float(row["high"]),  2),
        round(float(row["low"]),   2),
        round(float(row["close"]), 2),
        int(row["volume"]),
        now_utc,
    ))

    conn.commit()
    cur.close()
    print(f"[yfinance] ✅ Upserted 1 row for {exec_date} → {TABLE_NAME}")


# ── DAG definition ────────────────────────────────────────────
with DAG(
    dag_id       = "btc_historical_daily",
    default_args = {
        "owner":            "d226_team",
        "retries":          2,
        "retry_delay":      timedelta(minutes=5),
        "email_on_failure": False,
    },
    description  = "Fetch BTC daily OHLCV via yfinance → RAW.BTC_DAILY, then trigger btc_fng_daily",
    schedule     = "30 0 * * *",        # 00:30 UTC daily
    start_date   = datetime(2025, 1, 1),
    catchup      = False,
    tags         = ["btc", "d226", "historical", "yfinance"],
) as dag:

    t0_create = PythonOperator(
        task_id         = "create_table_if_not_exists",
        python_callable = create_table_if_not_exists,
    )

    t1_fetch = PythonOperator(
        task_id         = "fetch_and_load_historical",
        python_callable = fetch_and_load_historical,
    )

    # Trigger btc_fng_daily automatically after historical load succeeds
    t2_trigger_fng = TriggerDagRunOperator(
        task_id             = "trigger_btc_fng_daily",
        trigger_dag_id      = "btc_fng_daily",
        wait_for_completion = False,    # fire-and-forget, don't block this DAG
        reset_dag_run       = True,     # reset and re-run if already triggered today
    )

    t0_create >> t1_fetch >> t2_trigger_fng
