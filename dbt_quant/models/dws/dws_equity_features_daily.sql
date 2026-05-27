WITH returns AS (

    SELECT *
    FROM {{ ref('dws_equity_returns_daily') }}

),

base AS (

    SELECT
        security_id,
        ticker,
        date,
        adj_close,
        adj_volume,
        volume,
        div_cash,
        split_factor,

        ret_1d,
        ret_2d,
        ret_5d,
        ret_10d,
        ret_20d,
        ret_60d,
        ret_120d,

        adj_close * adj_volume AS dollar_volume,
        LN(1 + adj_close * adj_volume) AS log_dollar_volume

    FROM returns

),

rolling AS (

    SELECT
        security_id,
        ticker,
        date,
        adj_close,
        adj_volume,
        volume,
        div_cash,
        split_factor,

        ret_1d,
        ret_2d,
        ret_5d,
        ret_10d,
        ret_20d,
        ret_60d,
        ret_120d,

        LAG(ret_1d, 1) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) AS ret_1d_lag1,

        LAG(ret_1d, 2) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) AS ret_1d_lag2,

        LAG(ret_5d, 1) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) AS ret_5d_lag1,

        LAG(ret_5d, 2) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) AS ret_5d_lag2,

        LAG(ret_20d, 1) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) AS ret_20d_lag1,

        dollar_volume,
        log_dollar_volume,

        AVG(dollar_volume) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS avg_dollar_volume_20d_lagged,

        AVG(dollar_volume) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS avg_dollar_volume_60d_lagged,

        STDDEV(dollar_volume) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS std_dollar_volume_20d_lagged,

        AVG(adj_volume) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS avg_volume_20d_lagged,

        STDDEV(adj_volume) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS std_volume_20d_lagged,

        STDDEV(ret_1d) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS volatility_20d_lagged,

        STDDEV(ret_1d) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS volatility_60d_lagged,

        MAX(ret_1d) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS max_ret_20d_lagged,

        MIN(ret_1d) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS min_ret_20d_lagged,

        MAX(adj_close) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS high_20d_lagged,

        MIN(adj_close) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS low_20d_lagged,

        MAX(adj_close) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS high_60d_lagged,

        MIN(adj_close) OVER (
            PARTITION BY security_id
            ORDER BY date
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS low_60d_lagged,

        DATE_PART('dayofweek', date) AS day_of_week,
        DATE_PART('month', date) AS month

    FROM base

),

final AS (

    SELECT
        security_id,
        ticker,
        date,
        adj_close,
        adj_volume,
        volume,
        div_cash,
        split_factor,

        ret_1d,
        ret_2d,
        ret_5d,
        ret_10d,
        ret_20d,
        ret_60d,
        ret_120d,

        ret_1d_lag1,
        ret_1d_lag2,
        ret_5d_lag1,
        ret_5d_lag2,
        ret_20d_lag1,

        dollar_volume,
        log_dollar_volume,
        avg_dollar_volume_20d_lagged,
        avg_dollar_volume_60d_lagged,
        avg_volume_20d_lagged,

        CASE
            WHEN std_dollar_volume_20d_lagged IS NULL
                OR std_dollar_volume_20d_lagged = 0
            THEN NULL
            ELSE (dollar_volume - avg_dollar_volume_20d_lagged)
                / std_dollar_volume_20d_lagged
        END AS dollar_volume_zscore_20d_lagged,

        CASE
            WHEN std_volume_20d_lagged IS NULL
                OR std_volume_20d_lagged = 0
            THEN NULL
            ELSE (adj_volume - avg_volume_20d_lagged)
                / std_volume_20d_lagged
        END AS volume_zscore_20d_lagged,

        volatility_20d_lagged,
        volatility_60d_lagged,
        max_ret_20d_lagged,
        min_ret_20d_lagged,

        CASE
            WHEN high_20d_lagged IS NULL OR high_20d_lagged = 0
            THEN NULL
            ELSE adj_close / high_20d_lagged - 1
        END AS close_to_20d_high_lagged,

        CASE
            WHEN low_20d_lagged IS NULL OR low_20d_lagged = 0
            THEN NULL
            ELSE adj_close / low_20d_lagged - 1
        END AS close_to_20d_low_lagged,

        CASE
            WHEN high_60d_lagged IS NULL OR high_60d_lagged = 0
            THEN NULL
            ELSE adj_close / high_60d_lagged - 1
        END AS close_to_60d_high_lagged,

        CASE
            WHEN low_60d_lagged IS NULL OR low_60d_lagged = 0
            THEN NULL
            ELSE adj_close / low_60d_lagged - 1
        END AS close_to_60d_low_lagged,

        day_of_week,
        month

    FROM rolling

)

SELECT *
FROM final