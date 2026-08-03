# Week 9 � Reference Data Refresh and Dynamic Liquid Universe

## Objective

Week 9 builds the monthly reference-data and point-in-time liquidity-universe layer.

This work has two connected parts:

1. Monthly reference-data refresh.
2. Monthly point-in-time liquid universe construction.

The pipeline should distinguish between the broad operational coverage universe and the narrower research/trading universe.

## Coverage universe vs liquidity universe

### Coverage universe

The coverage universe is the broad operational universe used for price download.

It is built from:

- supported ticker data
- `dim_security`
- candidate-pool filters
- active/stale filters
- operational metadata such as unsupported symbols or skipped windows

The coverage universe is intentionally broad. It should not be limited to the current liquid universe, because future liquid-universe entrants need historical price and volume data before they become eligible.

Use the coverage universe for:

- Tiingo price download
- ODS raw files
- DWD price table
- Postgres ingestion metadata
- GCS and BigQuery DWD price storage

### Liquidity universe

The liquidity universe is the point-in-time research and trading universe.

It is derived from canonical DWD daily prices.

Use the liquidity universe for:

- factor computation
- cross-sectional ranking
- ML research panels
- labels
- strategy backtests
- candidate trades

## No-lookahead rule

Universe membership must avoid lookahead.

A membership month must be based only on data available before that month.

Example:

- June 2026 liquidity metrics define July 2026 membership.
- July 2026 partial data must not define July 2026 membership.

The initial implementation uses complete prior-month data only.

## Monthly reference-data refresh

Week 9 will add snapshot-aware monthly reference data:

- supported-tickers snapshot
- `dim_security` snapshot
- candidate-pool snapshot
- latest operational copy for daily scripts

Planned layout:

- `data/ods/source=tiingo/dataset=supported_tickers/snapshot_date=YYYY-MM-DD/`
- `data/dwd/security_master_snapshots/snapshot_date=YYYY-MM-DD/`
- `data/dwd/candidate_pool_snapshots/snapshot_date=YYYY-MM-DD/`

Latest operational files remain available for existing daily scripts:

- `data/dwd/security_master/dim_security.parquet`
- `data/dwd/security_master/candidate_security_pool.parquet`

## Monthly liquidity outputs

Planned local outputs:

- `data/dws/equity_liquidity_monthly/`
- `data/dwd/universe_membership_monthly/`

Initial universes:

- `us_liquid_100`
- `us_liquid_500`

Initial filters:

- median close >= 5
- trading-day coverage >= 80%
- median dollar volume >= 1,000,000
- positive volume required for dollar-volume ranking

## Day 1 status

Day 1 creates the config and design doc and profiles the current local data.

No reference snapshots, universe tables, DWD files, GCS objects, or BigQuery tables are modified on Day 1.

## Day 2 — Supported tickers and dim_security snapshots

Reference data is now snapshot-aware.

Supported ticker ingestion writes:

- dated snapshot: `data/ods/source=tiingo/dataset=supported_tickers/snapshot_date=YYYY-MM-DD/supported_tickers.csv`
- latest operational copy: `data/ods/source=tiingo/dataset=supported_tickers/supported_tickers.csv`

Security master build writes:

- dated snapshot: `data/dwd/security_master_snapshots/snapshot_date=YYYY-MM-DD/dim_security.parquet`
- latest operational copy: `data/dwd/security_master/dim_security.parquet`

The latest copy preserves compatibility with existing daily operational scripts.
The snapshoted copy provides auditability and monthly reference-data history.

## Day 3 — Candidate pool snapshots

Candidate pool generation is now snapshot-aware.

The candidate pool is the broad coverage universe used for operational price
download. It remains separate from the liquidity universe used for research and
strategy work.

Candidate pool build writes:

- dated snapshot: `data/dwd/candidate_pool_snapshots/snapshot_date=YYYY-MM-DD/candidate_security_pool.parquet`
- latest operational copy: `data/dwd/security_master/candidate_security_pool.parquet`

The latest copy preserves compatibility with existing daily operational scripts.
The snapshot copy provides auditability and monthly reference-data history.

For the July 2026 refresh, the candidate pool uses the `dim_security` snapshot
for snapshot date `2026-07-28`.

## Day 4 — Coverage universe integration

The daily price gap generator now uses the refreshed latest candidate pool as
the broad coverage universe.

Configured source:

- `data/dwd/security_master/candidate_security_pool.parquet`

This replaces the older bootstrap-candidates file as the default daily price
coverage input.

The candidate pool remains distinct from the liquidity universe:

- candidate pool / coverage universe: used for price download
- liquidity universe: used for research, factors, labels, and strategies

Day 4 validated price-gap generation in dry-run mode against the refreshed
candidate pool. No live Tiingo price download was run.

## Day 5 — Monthly liquidity metrics

Monthly liquidity metrics are built from canonical DWD daily prices.

Output:

- `data/dws/equity_liquidity_monthly/year=YYYY/month=MM/part-000.parquet`

The builder computes:

- expected trading days from the XNYS calendar
- trading day coverage
- median close
- median dollar volume
- average dollar volume
- p20 / p80 dollar volume
- zero-volume day count
- complete-month flag
- liquidity filter pass/fail flags
- liquidity score

Official metrics exclude incomplete months by default.

Initial filters:

- median close >= 5
- trading-day coverage >= 80%
- median dollar volume >= 1,000,000
- positive volume required

The June 2026 liquidity profile showed that these filters are appropriate for
a broad prefilter: roughly half of security-month rows pass, and the top 500
liquid names remain far above the minimum dollar-volume threshold.

## Day 6 — Point-in-time liquid universe membership

Point-in-time monthly liquid universe membership is now built from monthly
liquidity metrics.

Outputs:

- `data/dwd/universe_membership_monthly/universe_name=us_liquid_100/year=YYYY/month=MM/part-000.parquet`
- `data/dwd/universe_membership_monthly/universe_name=us_liquid_500/year=YYYY/month=MM/part-000.parquet`

Membership rule:

- `membership_month = source_metric_month + 1 month`

This avoids lookahead. For example, June 2026 liquidity metrics define July
2026 membership.

Ranking:

- score column: `median_dollar_volume`
- lookback months: 3
- aggregation: median

Initial universes:

- `us_liquid_100`
- `us_liquid_500`

The latest official membership month after Day 6 is expected to be July 2026
when the latest complete metric month is June 2026.