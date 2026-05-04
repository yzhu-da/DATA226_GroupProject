# BTC dbt Project

## Overview

This dbt project builds Bitcoin analytics models in Snowflake from three raw source tables:

- `BTC_DAILY`
- `BTC_FEAR_GREED`
- `BTC_REALTIME`

The project now covers three analytical layers:

- historical daily price analytics
- realtime / intraday market analytics
- forecast-vs-realtime validation

The project also includes dbt snapshots that preserve historical versions of key analytics models over time.

## Main Outputs

The primary dbt outputs are:

- `analytics.fct_btc_daily`
- `analytics.fct_btc_intraday_market`
- `analytics.btc_forecast_vs_realtime`
- `snapshot.snapshot_fct_btc_daily`
- `snapshot.snapshot_fct_btc_intraday_market`
- `snapshot.snapshot_btc_forecast_vs_realtime`

In business terms:

- `fct_btc_daily` is the core historical daily BTC table
- `fct_btc_intraday_market` is the intraday / realtime market feature table
- `btc_forecast_vs_realtime` compares a simple short-term forecast with observed realtime prices
- `snapshot_fct_btc_daily` tracks changes to the final historical daily table over time
- `snapshot_fct_btc_intraday_market` tracks changes to the intraday market analytics table over time
- `snapshot_btc_forecast_vs_realtime` tracks changes to the forecast-vs-realtime comparison table over time

## Project Structure

```text
btc/
├── dbt_project.yml
├── models/
│   ├── source.yml
│   ├── schema.yml
│   ├── transform/
│   │   ├── btc_daily_clean.sql
│   │   └── btc_fear_greed_clean.sql
│   └── analytics/
│       ├── fct_btc_daily.sql
│       ├── fct_btc_intraday_market.sql
│       └── btc_forecast_vs_realtime.sql
├── snapshots/
│   ├── snapshot_fct_btc_daily.sql
│   ├── snapshot_fct_btc_intraday_market.sql
│   └── snapshot_btc_forecast_vs_realtime.sql
├── tests/
│   ├── assert_btc_daily_price_consistency.sql
│   └── assert_btc_fear_greed_value_range.sql
├── macros/
├── seeds/
├── analyses/
└── target/
```

## File-by-File Explanation

### Project Configuration

#### `dbt_project.yml`

This is the main dbt project configuration file.

It defines:

- the project name
- the dbt profile name
- the folders dbt should use
- default materialization rules for models

In this project:

- models under `transform/` are built as views
- models under `analytics/` are built as tables
- the default example models are disabled

### Source Definition

#### `models/source.yml`

This file defines the non-dbt source tables that already exist in Snowflake.

It tells dbt:

- which schema the raw tables come from
- which tables are considered sources
- what those tables represent
- what source-level tests should run

In this project, `source.yml` defines:

- `raw.BTC_DAILY`
- `raw.BTC_FEAR_GREED`
- `gopher_raw.BTC_REALTIME`

`BTC_DAILY` and `BTC_FEAR_GREED` are used as the historical daily and sentiment sources. `BTC_REALTIME` is explicitly defined as a realtime source from `USER_DB_GOPHER.RAW.BTC_REALTIME`, which contains Elina's Binance-style intraday BTC market snapshots.

It also includes source tests such as:

- `not_null`
- `unique`
- `accepted_values`

### Model Documentation and Generic Tests

#### `models/schema.yml`

This file documents dbt models and defines model-level generic tests.

It describes:

- what each model is for
- which columns are important
- which columns must be unique or non-null
- which columns must only contain allowed values

In this project, it covers:

- `btc_daily_clean`
- `btc_fear_greed_clean`
- `fct_btc_daily`
- `fct_btc_intraday_market`
- `btc_forecast_vs_realtime`

### Transform Models

#### `models/transform/btc_daily_clean.sql`

This model cleans the raw `BTC_DAILY` table.

Its purpose is to:

- standardize column names
- cast fields into appropriate data types
- prepare daily BTC price history for downstream analytics

Important output columns include:

- `btc_date`
- `open_price_usd`
- `high_price_usd`
- `low_price_usd`
- `close_price_usd`
- `volume`

#### `models/transform/btc_fear_greed_clean.sql`

This model cleans the raw `BTC_FEAR_GREED` table.

Its purpose is to:

- standardize column names
- cast the sentiment score into numeric form
- preserve the sentiment label for reporting

Important output columns include:

- `btc_date`
- `fear_greed_value`
- `fear_greed_label`

### Analytics Models

#### `models/analytics/fct_btc_daily.sql`

This is the main historical daily analytics model.

It joins:

- `btc_daily_clean`
- `btc_fear_greed_clean`

on the daily date key.

Important output columns include:

- `btc_date`
- `open_price_usd`
- `high_price_usd`
- `low_price_usd`
- `close_price_usd`
- `volume`
- `fear_greed_value`
- `fear_greed_label`
- `day_price_change_usd`
- `day_return_pct`
- `intraday_range_pct`
- `record_updated_at`

#### Why `record_updated_at` exists

The `record_updated_at` column was added to support dbt snapshots.

It represents the latest known update time for each final analytics row and allows dbt to detect when the row has changed.

#### `models/analytics/fct_btc_intraday_market.sql`

This model builds an intraday / realtime BTC analytics table from the source `gopher_raw.BTC_REALTIME`, which points to `USER_DB_GOPHER.RAW.BTC_REALTIME`.

Its purpose is to:

- preserve one row per realtime snapshot
- expose intraday market activity metrics
- create derived signals that can be used for realtime monitoring and downstream comparison

Important output columns include:

- `fetched_at`
- `snapshot_date`
- `snapshot_hour`
- `open`
- `high`
- `low`
- `close`
- `volume_btc`
- `volume_usdt`
- `trades`
- `taker_buy_ratio`
- `price_spread_usd`
- `snapshot_return_pct`
- `volatility_pct`
- `avg_trade_size_usdt`
- `snapshot_direction`
- `buy_pressure_level`
- `record_updated_at`

#### `models/analytics/btc_forecast_vs_realtime.sql`

This model compares a rolling short-term BTC forecast against actual observed realtime market data.

Its purpose is to:

- use `fct_btc_daily` as the historical daily forecast input
- calculate a rolling 7-record average return for each eligible historical base date
- forecast BTC close prices for multiple future days from each base date
- compare forecasted values and forecast direction with realtime observations from `fct_btc_intraday_market`
- keep one forecast per `prediction_date` by selecting the forecast generated from the most recent available `base_date`

This model depends on both:

- `fct_btc_daily`
- `fct_btc_intraday_market`

Therefore, both the historical daily model and the intraday realtime model must exist before the forecast comparison model can run successfully.

The model uses a rolling baseline approach rather than a production-grade trading model. For each base date after the first 7 historical records, it uses the previous 7 historical daily returns to compute `avg_return_7d`. It then forecasts multiple future days using compound return logic:

predicted_close = base_close * (1 + avg_return_7d) ^ day_ahead

Because multiple historical base dates can generate the same prediction_date, the model keeps only the forecast generated from the latest available base_date. This avoids duplicate prediction dates and preserves the most recent historical signal.

Important output columns include:

- `prediction_date`
- `day_ahead`
- `base_date`
- `base_close`
- `predicted_close`
- `actual_realtime_close`
- `actual_fetched_at`
- `actual_snapshot_hour`
- `error_usd`
- `error_pct`
- `predicted_direction`
- `actual_direction`
- `direction_match`
- `avg_return_7d_pct`
- `base_fear_greed_value`
- `base_fear_greed_label`
- `base_intraday_range_pct`
- `base_volume`
- `realtime_volatility_pct`
- `buy_pressure_level`
- `taker_buy_ratio`
- `validation_status`
- `model_name`
- `model_created_at`
- `record_updated_at`

The validation_status field indicates whether a forecast has a matching realtime observation:

validated_with_realtime: realtime actual close is available for the prediction date
forecast_only: no realtime actual close is currently available for the prediction date

This design allows the table to support both current validation and future validation as the realtime pipeline continues to collect new snapshots.


### Snapshot Definition

#### `snapshots/snapshot_fct_btc_daily.sql`

This file defines a dbt snapshot on top of the final daily analytics model.

Its purpose is to:

- preserve historical versions of `fct_btc_daily`
- capture row-level changes over time
- support the dbt snapshot workflow covered in class

This snapshot uses:

- `btc_date` as the unique key
- `record_updated_at` as the update timestamp
- `timestamp` strategy for change detection
- `snapshot` as the target schema

After running `dbt snapshot`, dbt creates a snapshot table containing the analytics columns plus snapshot metadata such as:

- `dbt_scd_id`
- `dbt_updated_at`
- `dbt_valid_from`
- `dbt_valid_to`

The snapshot output should be created in the `snapshot` schema, not in `analytics`.

#### `snapshots/snapshot_fct_btc_intraday_market.sql`

This file defines a dbt snapshot for the intraday / realtime analytics table.

Its purpose is to:

- preserve historical versions of `fct_btc_intraday_market`
- track changes in intraday market metrics over time
- retain versioned records in the `snapshot` schema

This snapshot uses:

- `fetched_at` as the unique key
- `record_updated_at` as the update timestamp
- `timestamp` strategy for change detection

#### `snapshots/snapshot_btc_forecast_vs_realtime.sql`

This file defines a dbt snapshot for the forecast comparison model.

Its purpose is to:

- preserve historical versions of `btc_forecast_vs_realtime`
- track how forecast comparison rows change as realtime observations become available
- retain versioned records in the `snapshot` schema

This snapshot uses:

- `prediction_date` as the unique key
- `record_updated_at` as the update timestamp
- `timestamp` strategy for change detection

Because `btc_forecast_vs_realtime` keeps only one row per `prediction_date`, `prediction_date` can safely be used as the snapshot unique key.

### Singular Data Tests

#### `tests/assert_btc_daily_price_consistency.sql`

This is a custom business-rule test for daily BTC prices.

It checks whether the OHLC values are logically consistent, for example:

- prices should be positive
- `high` should not be below `open`, `close`, or `low`
- `low` should not be above `open`, `close`, or `high`

#### `tests/assert_btc_fear_greed_value_range.sql`

This is a custom business-rule test for sentiment scores.

It checks whether `fear_greed_value` stays within the expected range:

- minimum: `0`
- maximum: `100`

### Supporting Folders

#### `snapshots/`

This folder stores dbt snapshot definitions.

In this project, it contains:

- `snapshot_fct_btc_daily.sql`
- `snapshot_fct_btc_intraday_market.sql`
- `snapshot_btc_forecast_vs_realtime.sql`

#### `macros/`

This folder is reserved for reusable Jinja macros.

It is currently empty because the project does not need custom macros yet.

#### `seeds/`

This folder is reserved for static CSV seed files.

It is currently empty because all data comes directly from Snowflake source tables.

#### `analyses/`

This folder is reserved for one-off analytical SQL files.

It is currently empty because the core logic has been placed into dbt models instead.

#### `target/`

This folder is automatically generated by dbt.

It stores compiled SQL, manifests, documentation artifacts, and run metadata.

Common files inside `target/` include:

- `manifest.json`
- `catalog.json`
- `run_results.json`

These files are generated by dbt and should be treated as build artifacts, not hand-written project logic.

## What the Project Builds

After a successful run, the main objects produced by this project are:

- `analytics.btc_daily_clean`
- `analytics.btc_fear_greed_clean`
- `analytics.fct_btc_daily`
- `analytics.fct_btc_intraday_market`
- `analytics.btc_forecast_vs_realtime`
- `snapshot.snapshot_fct_btc_daily`
- `snapshot.snapshot_fct_btc_intraday_market`
- `snapshot.snapshot_btc_forecast_vs_realtime`

## How to Run the Project

### 1. Validate the project

```bash
dbt parse
```

This checks whether the project structure, SQL, model references, and snapshot definitions are valid.

### 2. Build all models and run tests

```bash
dbt build
```

This command runs the full pipeline in dependency order:

- builds models
- runs generic tests
- runs singular tests

### 3. Build the historical daily model first

```bash
dbt build --select fct_btc_daily
```

This step ensures the historical daily table exists before the forecast model reads from it.

### 4. Build only the new realtime and forecast models

```bash
dbt build --select fct_btc_intraday_market btc_forecast_vs_realtime
```

This is useful when validating the realtime analytics and forecast validation branch without rebuilding everything else.

This order matters because `btc_forecast_vs_realtime` reads from:

- `fct_btc_daily`
- `fct_btc_intraday_market`

If the project is being built from scratch, run the upstream historical models first or use the full project build:

```bash
dbt build
```
If only validating the forecast branch after upstream models already exist, use:

```bash
dbt build --select fct_btc_intraday_market btc_forecast_vs_realtime
```

### 5. Run the snapshot

```bash
dbt snapshot
```

This command records historical versions of all snapshot-enabled models in the `snapshots/` folder.

It should normally be run after `dbt build`, because the snapshots read from analytics models that must already exist.

If an older copy of a snapshot table was accidentally created in the `analytics` schema, it should be dropped so that snapshot outputs only exist in the `snapshot` schema.

### 6. Generate documentation

```bash
dbt docs generate
```

This creates dbt documentation artifacts and lineage metadata.

### 7. Open the dbt docs site

```bash
dbt docs serve
```

This starts a local documentation website where you can view:

- models
- sources
- column descriptions
- test metadata
- lineage graphs

## Lineage Summary

The main model relationships in this project are:

```text
raw.BTC_DAILY --------> btc_daily_clean --------\
                                                 -> fct_btc_daily -----> snapshot_fct_btc_daily
raw.BTC_FEAR_GREED --> btc_fear_greed_clean ----/

gopher_raw.BTC_REALTIME -----------------------> fct_btc_intraday_market ---> btc_forecast_vs_realtime
                                                     |                              |
                                                     v                              v
                                           snapshot_fct_btc_intraday_market   snapshot_btc_forecast_vs_realtime
fct_btc_daily ----------------------------------------------------------------> btc_forecast_vs_realtime
```

## Notes

- The project contains historical daily analytics, realtime / intraday analytics, and forecast-vs-realtime validation.
- `fct_btc_daily` remains a daily-grain table.
- `fct_btc_intraday_market` remains a snapshot-grain table.
- `btc_forecast_vs_realtime` is a rolling baseline forecast comparison model, not a production trading model.
- The forecast model uses the previous 7 historical daily returns to generate multi-day BTC close forecasts.
- When realtime observations are available for a `prediction_date`, the model compares predicted close against actual realtime close.
- When realtime observations are not yet available, the row remains forecast-only.
- The snapshot layer preserves historical versions of all three analytics models over time.
