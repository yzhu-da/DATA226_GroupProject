# ============================================================
# DAG 4: BTC Binance 4-Hourly Kline Snapshot
# Source:   Binance API (no API key required) + CoinGecko (market cap)
# Table:    USER_DB_GOPHER.RAW.BTC_BINANCE
# Schedule: Every 4 hours (00:00 / 04:00 / 08:00 / 12:00 / 16:00 / 20:00 UTC)
# Purpose:  Track intraday changes in taker_buy_ratio and market structure
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import requests

# ── Config ────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_DB      = "USER_DB_GOPHER"
SNOWFLAKE_SCHEMA  = "RAW"
TABLE_NAME        = "BTC_REALTIME"


# ── Task 1: Auto-create table ─────────────────────────────────
def create_table_if_not_exists(**context):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()
    cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            fetched_at          TIMESTAMP_NTZ NOT NULL PRIMARY KEY,
            snapshot_date       DATE,
            snapshot_hour       INT,
            open                FLOAT,
            high                FLOAT,
            low                 FLOAT,
            close               FLOAT,
            volume_btc          FLOAT,
            volume_usdt         FLOAT,
            trades              BIGINT,
            taker_buy_volume    FLOAT,
            taker_buy_quote     FLOAT,
            taker_buy_ratio     FLOAT,
            market_cap_usd      BIGINT,
            inserted_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    conn.commit()
    cur.close()
    print(f"[{TABLE_NAME}] ✅ Table ready")


# ── Task 2: Fetch Binance kline + CoinGecko market cap ────────
def fetch_and_load_binance(**context):
    now_utc       = datetime.now(timezone.utc)
    fetched_at    = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    snapshot_date = str(now_utc.date())
    snapshot_hour = now_utc.hour   # 0 / 4 / 8 / 12 / 16 / 20

    api_key = Variable.get("COINGECKO_API_KEY", default_var="")

    # ── Binance kline（当天从 00:00 UTC 开始的累计日线数据）──
    start_ms = int(datetime(
        now_utc.year, now_utc.month, now_utc.day,
        tzinfo=timezone.utc
    ).timestamp() * 1000)

    print(f"[Binance] Fetching BTCUSDT 1d kline snapshot at {fetched_at} UTC")

    binance_resp = requests.get(
         "https://api.binance.us/api/v3/klines",
        params={
            "symbol":    "BTCUSDT",
            "interval":  "1d",
            "startTime": start_ms,
            "limit":     1,
        },
        timeout=30,
    )
    binance_resp.raise_for_status()
    data = binance_resp.json()

    if not data:
        raise ValueError(f"[Binance] No data returned for {snapshot_date}")

    row           = data[0]
    volume_btc    = float(row[5])
    taker_buy_vol = float(row[9])
    taker_buy_ratio = round(taker_buy_vol / volume_btc, 6) if volume_btc > 0 else None

    print(f"[Binance] close={row[4]} taker_buy_ratio={taker_buy_ratio} hour={snapshot_hour}")

    # ── CoinGecko market cap ───────────────────────────────────
    print(f"[CoinGecko] Fetching market cap...")

    headers = {"x-cg-demo-api-key": api_key} if api_key else {}
    cg_resp = requests.get(
        "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart",
        headers=headers,
        params={
            "vs_currency": "usd",
            "days":        "1",
            "interval":    "daily",
        },
        timeout=30,
    )
    cg_resp.raise_for_status()
    market_cap = int(cg_resp.json()["market_caps"][-1][1])

    print(f"[CoinGecko] market_cap={market_cap}")

    # ── Write to Snowflake ─────────────────────────────────────
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()
    cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    # INSERT（fetched_at unique，every insert will generate a new record）
    cur.execute(f"""
        INSERT INTO {TABLE_NAME} (
            fetched_at, snapshot_date, snapshot_hour,
            open, high, low, close,
            volume_btc, volume_usdt, trades,
            taker_buy_volume, taker_buy_quote,
            taker_buy_ratio, market_cap_usd
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s
        )
    """, (
        fetched_at, snapshot_date, snapshot_hour,
        round(float(row[1]), 2),    # open
        round(float(row[2]), 2),    # high
        round(float(row[3]), 2),    # low
        round(float(row[4]), 2),    # close
        round(volume_btc,    6),    # volume_btc
        round(float(row[7]), 2),    # volume_usdt
        int(row[8]),                # trades
        round(taker_buy_vol, 6),    # taker_buy_volume
        round(float(row[10]), 2),   # taker_buy_quote
        taker_buy_ratio,            # taker_buy_ratio
        market_cap,                 # market_cap_usd
    ))

    conn.commit()
    cur.close()
    print(f"[BTC_BINANCE] ✅ Inserted snapshot at {fetched_at} (hour={snapshot_hour})")


# ── DAG definition ────────────────────────────────────────────
with DAG(
    dag_id       = "btc_binance_daily",
    default_args = {
        "owner":            "d226_team",
        "retries":          2,
        "retry_delay":      timedelta(minutes=5),
        "email_on_failure": False,
    },
    description  = "Fetch BTC Binance kline every 4h → RAW.BTC_BINANCE (intraday tracking)",
    schedule     = "0 */4 * * *",      # every 4h：00/04/08/12/16/20 UTC
    start_date   = datetime(2025, 1, 1),
    catchup      = False,
    tags         = ["btc", "d226", "binance"],
) as dag:

    t0_create = PythonOperator(
        task_id         = "create_table_if_not_exists",
        python_callable = create_table_if_not_exists,
    )

    t1_fetch = PythonOperator(
        task_id         = "fetch_and_load_binance",
        python_callable = fetch_and_load_binance,
    )

    t0_create >> t1_fetch
