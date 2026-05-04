# SJSU DATA226 2026 Spring Group Project
Group 6: Xu Wang, Xuanhua Li, Ying Zhu, Elina Yin

## 1. Project and Dataset Overview

## 2. ETL Part

## 3. ELT Part

The ELT portion of this project is implemented with dbt and Snowflake.

All dbt-related files are stored in the `btc/` folder.

### 3.1 ELT Scope

The ELT workflow is built from three raw source tables:

- `BTC_DAILY`
- `BTC_FEAR_GREED`
- `BTC_REALTIME`

`BTC_DAILY` and `BTC_FEAR_GREED` support the historical daily analytics layer. `BTC_REALTIME` is the realtime / intraday BTC source table from `USER_DB_GOPHER.RAW.BTC_REALTIME`, which is used to build the realtime market analytics and forecast validation layer.

The ELT work covers three analytical layers:

- historical daily price analytics
- realtime / intraday market analytics
- forecast-vs-realtime validation

The ELT work also includes dbt snapshots for historical version tracking.

### 3.2 ELT Folder Location

The `btc/` folder contains the full dbt project, including:

- `dbt_project.yml`
- `models/`
- `snapshots/`
- `tests/`
- dbt-specific documentation in `btc/README.md`

### 3.3 ELT Main Outputs

The primary dbt outputs are:

- `analytics.fct_btc_daily`
- `analytics.fct_btc_intraday_market`
- `analytics.btc_forecast_vs_realtime`
- `snapshot.snapshot_fct_btc_daily`
- `snapshot.snapshot_fct_btc_intraday_market`
- `snapshot.snapshot_btc_forecast_vs_realtime`

These outputs serve different purposes:

- `fct_btc_daily` is the final historical daily analytics table
- `fct_btc_intraday_market` is the realtime / intraday feature table
- `btc_forecast_vs_realtime` is the forecast validation table
- the three snapshot tables preserve historical versions of the analytics outputs over time

### 3.4 ELT Model Summary

#### Historical Daily Analytics

`fct_btc_daily` is the core historical daily BTC table.

It combines:

- cleaned BTC daily OHLCV data
- cleaned Fear & Greed daily sentiment
- derived daily metrics such as return and intraday range

Important fields in `fct_btc_daily` include:

- `btc_date`: the daily date key
- `open_price_usd`: BTC opening price for the day
- `high_price_usd`: BTC highest price for the day
- `low_price_usd`: BTC lowest price for the day
- `close_price_usd`: BTC closing price for the day
- `volume`: BTC daily trading volume from the historical source
- `fear_greed_value`: numeric sentiment score
- `fear_greed_label`: sentiment category label
- `day_price_change_usd`: difference between close and open
- `day_return_pct`: daily return percentage
- `intraday_range_pct`: percentage range between the daily high and low
- `record_updated_at`: timestamp used for snapshot version tracking

#### Realtime / Intraday Analytics

`fct_btc_intraday_market` is the intraday market feature table built from the realtime source `USER_DB_GOPHER.RAW.BTC_REALTIME`.

It includes metrics such as:

- intraday price spread
- snapshot return
- volatility percentage
- average trade size
- buy pressure level

Important fields in `fct_btc_intraday_market` include:

- `fetched_at`: timestamp when the realtime snapshot was collected
- `snapshot_date`: calendar date of the snapshot
- `snapshot_hour`: hour of the snapshot
- `open`: opening price for that intraday snapshot window
- `high`: highest price within that snapshot window
- `low`: lowest price within that snapshot window
- `close`: closing price for that snapshot window
- `volume_btc`: traded BTC volume
- `volume_usdt`: traded USDT volume
- `trades`: number of trades
- `taker_buy_volume`: taker buy volume in BTC
- `taker_buy_quote`: taker buy quote volume
- `taker_buy_ratio`: ratio of taker buy volume to total traded volume
- `market_cap_usd`: market capitalization in USD
- `price_spread_usd`: difference between high and low
- `snapshot_return_pct`: return from open to close within the snapshot
- `volatility_pct`: snapshot volatility percentage
- `avg_trade_size_usdt`: average trade size in USDT
- `snapshot_direction`: whether the snapshot moved up, down, or stayed neutral
- `buy_pressure_level`: derived buy-pressure classification
- `record_updated_at`: timestamp used for snapshot version tracking

#### Forecast Validation

`btc_forecast_vs_realtime` compares a rolling short-term BTC baseline forecast against observed realtime market data.

It:

- uses `fct_btc_daily` as the historical daily forecast input
- computes a rolling 7-record average return from prior historical daily records
- predicts BTC close prices for multiple future days from each eligible historical base date
- compares predicted values and predicted direction to actual realtime observations from `fct_btc_intraday_market`
- keeps one forecast per `prediction_date` by selecting the forecast generated from the most recent available `base_date`

This model depends on both:

- `fct_btc_daily`
- `fct_btc_intraday_market`

Therefore, both the historical daily model and the realtime intraday model must already exist before the forecast validation model can run successfully.

The model uses a baseline forecast formula:

predicted_close = base_close * (1 + avg_return_7d) ^ day_ahead

This is not intended to be a production trading model. Its purpose is to provide a simple historical-trend expectation that can be validated against newly observed realtime BTC market data.

Important fields in `btc_forecast_vs_realtime` include:

- `prediction_date`: forecast target date
- `day_ahead`: how many days ahead the forecast is made
- `base_date`: latest historical date used as the forecast baseline
- `base_close`: BTC closing price on the base date
- `predicted_close`: forecasted BTC close price
- `actual_realtime_close`: observed realtime close price for comparison
- `actual_fetched_at`: timestamp of the realtime observation used for comparison
- `actual_snapshot_hour`: snapshot hour of the observed realtime record
- `error_usd`: absolute forecast error in USD
- `error_pct`: percentage forecast error
- `predicted_direction`: predicted market direction
- `actual_direction`: observed market direction
- `direction_match`: whether predicted and actual direction match
- `avg_return_7d_pct`: trailing 7-day average return used in the model
- `base_fear_greed_value`: latest historical Fear & Greed numeric value
- `base_fear_greed_label`: latest historical Fear & Greed label
- `base_intraday_range_pct`: historical intraday range percentage on the base date
- `base_volume`: historical volume on the base date
- `realtime_volatility_pct`: volatility observed from realtime data
- `buy_pressure_level`: buy-pressure classification observed from realtime data
- `taker_buy_ratio`: realtime taker buy ratio
- `validation_status`: whether the forecast has a matching realtime observation
- `model_name`: name of the forecasting method
- `model_created_at`: timestamp when the forecast model row was generated
- `record_updated_at`: timestamp used for snapshot version tracking

### 3.5 ELT Snapshots

The project uses dbt snapshots to preserve historical versions of analytics tables in the `snapshot` schema.

Snapshot outputs include:

- `snapshot_fct_btc_daily`
- `snapshot_fct_btc_intraday_market`
- `snapshot_btc_forecast_vs_realtime`

Each snapshot table stores the original model columns together with dbt snapshot metadata such as:

- `dbt_scd_id`
- `dbt_updated_at`
- `dbt_valid_from`
- `dbt_valid_to`

### 3.6 ELT Tests

The ELT portion includes:

- source-level tests
- model-level generic tests
- custom singular tests for price consistency and sentiment value range

### 3.7 How to Run the ELT Project

Validate the project:

```bash
dbt parse
```

Build models and run tests:

```bash
dbt build
```

Build the historical daily model first:

```bash
dbt build --select fct_btc_daily
```

Then build the realtime and forecast validation models:

```bash
dbt build --select fct_btc_intraday_market btc_forecast_vs_realtime
```

This order matters because `btc_forecast_vs_realtime` reads from both:

- `fct_btc_daily`
- `fct_btc_intraday_market`

If the project is being built from scratch, use the full project build so that upstream dependencies are created first:

```bash
dbt build
```

If upstream models already exist and only the realtime / forecast branch needs to be refreshed, use:

```bash
dbt build --select fct_btc_intraday_market btc_forecast_vs_realtime
```

Run snapshots:

```bash
dbt snapshot
```

Generate documentation:

```bash
dbt docs generate
```

Serve documentation locally:

```bash
dbt docs serve
```

### 3.8 ELT Notes

- `fct_btc_daily` is a daily-grain table.
- `fct_btc_intraday_market` is a snapshot-grain table.
- `btc_forecast_vs_realtime` is a rolling baseline forecast validation model for analysis, not a production trading model.
- The forecast model uses prior 7-record historical average returns to generate multi-day BTC close forecasts.
- Forecast rows with matching realtime observations are marked as validated; rows without available realtime observations remain forecast-only.
- Snapshot tables should be created in the `snapshot` schema, not in `analytics`.

## 4. Dashboard
