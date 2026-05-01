# BTC dbt Project

## Overview

This dbt project builds a historical Bitcoin analytics table from two raw Snowflake source tables:

- `BTC_DAILY`
- `BTC_FEAR_GREED`

The final output is a daily analytics table that combines:

- daily BTC OHLCV price history
- daily Fear & Greed sentiment
- simple derived metrics such as daily return and intraday range

The project also includes a dbt snapshot that preserves historical versions of the final analytics table.

This project intentionally focuses on the historical daily layer only.  
Realtime models were removed from the final version because their grain and coverage did not align with the historical daily dataset.

## Final Output

The main analytics model is:

- `fct_btc_daily`

This table contains one row per day and is intended to be the final table for analysis, reporting, and screenshots.

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
│       └── fct_btc_daily.sql
├── snapshots/
│   └── snapshot_fct_btc_daily.sql
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

### Analytics Model

#### `models/analytics/fct_btc_daily.sql`

This is the final analytics model.

It joins:

- `btc_daily_clean`
- `btc_fear_greed_clean`

on the daily date key.

This model creates the final historical BTC daily table used for analysis.

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

### Snapshot Definition

#### `snapshots/snapshot_fct_btc_daily.sql`

This file defines a dbt snapshot on top of the final analytics model.

Its purpose is to:

- preserve historical versions of `fct_btc_daily`
- capture row-level changes over time
- support the dbt snapshot workflow covered in class

This snapshot uses:

- `btc_date` as the unique key
- `record_updated_at` as the update timestamp
- `timestamp` strategy for change detection

After running `dbt snapshot`, dbt creates a snapshot table containing the analytics columns plus snapshot metadata such as:

- `dbt_scd_id`
- `dbt_updated_at`
- `dbt_valid_from`
- `dbt_valid_to`

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

The snapshot captures historical versions of the final analytics table.

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

- `btc_daily_clean`
- `btc_fear_greed_clean`
- `fct_btc_daily`
- `snapshot_fct_btc_daily`

From a business perspective:

- `btc_daily_clean` is the cleaned daily BTC price layer
- `btc_fear_greed_clean` is the cleaned daily sentiment layer
- `fct_btc_daily` is the final daily historical analytics table
- `snapshot_fct_btc_daily` is the historical version-tracking table for the final analytics output

## How to Run the Project

### 1. Validate the project

```bash
dbt parse
```

This checks whether the project structure, SQL, and model references are valid.

### 2. Build models and run tests

```bash
dbt build
```

This command runs the full pipeline in dependency order:

- builds models
- runs generic tests
- runs singular tests

### 3. Run the snapshot

```bash
dbt snapshot
```

This command records historical versions of `fct_btc_daily`.

It should normally be run after `dbt build`, because the snapshot reads from the final analytics model.

### 4. Generate documentation

```bash
dbt docs generate
```

This creates dbt documentation artifacts and lineage metadata.

### 5. Open the dbt docs site

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

The final lineage for this project is:

```text
raw.BTC_DAILY --------> btc_daily_clean --------\
                                                 -> fct_btc_daily
raw.BTC_FEAR_GREED --> btc_fear_greed_clean ----/

fct_btc_daily ----------------------------------> snapshot_fct_btc_daily
```

## Notes

- This final version does not use the realtime BTC table in the analytics model.
- The reason is that the realtime dataset did not provide historical daily coverage aligned with the historical BTC daily table.
- The final design keeps a clean and consistent grain: one row per day.
- The snapshot layer was added to preserve historical versions of the final analytics table over time.
