WITH prices AS (

    SELECT
        security_id,
        ticker,
        date,
        adj_close,
        adj_volume,
        volume,
        div_cash,
        split_factor
    FROM {{ ref('stg_tiingo__equity_price_daily') }}

),

returns AS (

    SELECT
        security_id,
        ticker,
        date,
        adj_close,
        adj_volume,
        volume,
        div_cash,
        split_factor,

        adj_close / LAG(adj_close, 1) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) - 1 AS ret_1d,

        adj_close / LAG(adj_close, 2) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) - 1 AS ret_2d,

        adj_close / LAG(adj_close, 5) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) - 1 AS ret_5d,

        adj_close / LAG(adj_close, 10) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) - 1 AS ret_10d,

        adj_close / LAG(adj_close, 20) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) - 1 AS ret_20d,

        adj_close / LAG(adj_close, 60) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) - 1 AS ret_60d,

        adj_close / LAG(adj_close, 120) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) - 1 AS ret_120d

    FROM prices

)

SELECT *
FROM returns