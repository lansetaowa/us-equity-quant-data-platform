BEGIN;

DELETE FROM metadata.datasets
WHERE dataset_name IN (
    'equity_price_daily_sample',
    'supported_tickers',
    'dim_security',
    'candidate_security_pool',
    'equity_price_daily',
    'equity_liquidity_monthly',
    'universe_membership_monthly'
);

INSERT INTO metadata.datasets (
    dataset_name,
    layer,
    storage_path,
    description,
    created_at,
    updated_at
)
VALUES
(
    'supported_tickers',
    'ods',
    'data/ods/source=tiingo/dataset=supported_tickers/',
    'Tiingo supported tickers raw latest file and dated snapshots.',
    NOW(),
    NOW()
),
(
    'dim_security',
    'dwd',
    'data/dwd/security_master/dim_security.parquet; data/dwd/security_master_snapshots/',
    'Normalized security master derived from Tiingo supported tickers.',
    NOW(),
    NOW()
),
(
    'candidate_security_pool',
    'dwd',
    'data/dwd/security_master/candidate_security_pool.parquet; data/dwd/candidate_pool_snapshots/',
    'Broad operational coverage universe used for price download task generation.',
    NOW(),
    NOW()
),
(
    'equity_price_daily',
    'dwd',
    'data/dwd/equity_price_daily/; gs://<GCS_BUCKET>/dwd/equity_price_daily/; quant_dwh.dwd_equity_price_daily',
    'Canonical normalized daily equity price fact table.',
    NOW(),
    NOW()
),
(
    'equity_liquidity_monthly',
    'dws',
    'data/dws/equity_liquidity_monthly/; gs://<GCS_BUCKET>/dws/equity_liquidity_monthly/; quant_dwh.dws_equity_liquidity_monthly',
    'Monthly liquidity metrics derived from DWD daily prices.',
    NOW(),
    NOW()
),
(
    'universe_membership_monthly',
    'dwd',
    'data/dwd/universe_membership_monthly/; gs://<GCS_BUCKET>/dwd/universe_membership_monthly/; quant_dwh.dim_universe_membership_monthly',
    'Point-in-time monthly liquid universe membership for research and strategy layers.',
    NOW(),
    NOW()
);

COMMIT;