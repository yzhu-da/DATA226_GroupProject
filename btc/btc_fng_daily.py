# ============================================================
# DAG 3: BTC Fear & Greed Index (daily)
# Source:   Alternative.me (no API key required)
# Table:    USER_DB_GOPHER.RAW.BTC_FEAR_GREED
# Schedule: None — triggered automatically by btc_historical_daily
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta, timezone
import requests

# ── Config ────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_DB      = "USER_DB_GOPHER"
SNOWFLAKE_SCHEMA  = "RAW"
TABLE_NAME        = "BTC_FEAR_GREED"


# ── Task 1: Auto-create table ─────────────────────────────────
def create_table_if_not_exists(**context):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()
    cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date        DATE          NOT NULL PRIMARY KEY,
            fng_value   INT,
            fng_label   VARCHAR(50),
            fetched_at  TIMESTAMP_NTZ,
            inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    conn.commit()
    cur.close()
    print(f"[{TABLE_NAME}] ✅ Table ready")


# ── Task 2: Fetch Fear & Greed Index and load to Snowflake ───
def fetch_and_load_fng(**context):
    # When triggered by TriggerDagRunOperator, logical_date = trigger time
    # We derive yesterday's date from it to stay consistent with historical DAG
    logical_date = context.get("data_interval_start") or context.get("logical_date")
    exec_date    = logical_date.date() if hasattr(logical_date, "date") else datetime.now(timezone.utc).date() - timedelta(days=1)
    fetched_at   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    print(f"[Alternative.me] Fetching Fear & Greed Index for {exec_date}")

    # Fetch last 2 days to maximise chance of capturing yesterday's data
    resp = requests.get(
        "https://api.alternative.me/fng/?limit=2",
        timeout=30
    )
    resp.raise_for_status()
    raw = resp.json()

    # Try to match yesterday's date exactly
    target = None
    for item in raw["data"]:
        d = datetime.fromtimestamp(int(item["timestamp"]), timezone.utc).date()
        if d == exec_date:
            target = item
            break

    # Fallback: API sometimes only returns today's data
    if target is None:
        print(
            f"[Alternative.me] ⚠️  No exact match for {exec_date}. "
            "Using latest available record."
        )
        target = raw["data"][0]

    record = {
        "date":       exec_date.strftime("%Y-%m-%d"),
        "fng_value":  int(target["value"]),
        "fng_label":  target["value_classification"],
        "fetched_at": fetched_at,
    }

    print(
        f"[Alternative.me] date={record['date']} | "
        f"value={record['fng_value']} | label={record['fng_label']}"
    )

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
            fng_value  = %s,
            fng_label  = %s,
            fetched_at = %s
        WHEN NOT MATCHED THEN INSERT
            (date, fng_value, fng_label, fetched_at)
        VALUES (%s, %s, %s, %s)
    """, (
        record["date"],
        record["fng_value"], record["fng_label"], record["fetched_at"],
        record["date"], record["fng_value"], record["fng_label"], record["fetched_at"],
    ))

    conn.commit()
    cur.close()
    print(f"[Alternative.me] ✅ Upserted row for {exec_date} → {TABLE_NAME}")


# ── DAG definition ────────────────────────────────────────────
with DAG(
    dag_id       = "btc_fng_daily",
    default_args = {
        "owner":            "d226_team",
        "retries":          3,              # extra retry — free API can be flaky
        "retry_delay":      timedelta(minutes=5),
        "email_on_failure": False,
    },
    description  = "Fetch BTC Fear & Greed Index via Alternative.me → RAW.BTC_FEAR_GREED (triggered by btc_historical_daily)",
    schedule     = None,                   # triggered by btc_historical_daily, not by time
    start_date   = datetime(2025, 1, 1),
    catchup      = False,
    tags         = ["btc", "d226", "sentiment", "fear-greed"],
) as dag:

    t0_create = PythonOperator(
        task_id         = "create_table_if_not_exists",
        python_callable = create_table_if_not_exists,
    )

    t1_fetch = PythonOperator(
        task_id         = "fetch_and_load_fng",
        python_callable = fetch_and_load_fng,
    )

    t0_create >> t1_fetch
