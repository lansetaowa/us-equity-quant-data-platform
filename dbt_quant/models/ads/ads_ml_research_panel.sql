WITH features AS (

    SELECT *
    FROM {{ ref('dws_equity_features_daily') }}

),

labels_raw AS (

    SELECT
        security_id,
        ticker,
        date,

        LEAD(adj_close, 1) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) / adj_close - 1 AS fwd_ret_1d,

        LEAD(adj_close, 5) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) / adj_close - 1 AS fwd_ret_5d,

        LEAD(adj_close, 20) OVER (
            PARTITION BY security_id
            ORDER BY date
        ) / adj_close - 1 AS fwd_ret_20d

    FROM features

),

labels AS (

    SELECT
        security_id,
        ticker,
        date,
        fwd_ret_1d,
        fwd_ret_5d,
        fwd_ret_20d,

        CASE
            WHEN fwd_ret_5d IS NULL THEN NULL
            WHEN fwd_ret_5d > 0 THEN 1
            ELSE 0
        END AS label_direction_5d,

        CASE
            WHEN fwd_ret_20d IS NULL THEN NULL
            WHEN fwd_ret_20d > 0 THEN 1
            ELSE 0
        END AS label_direction_20d

    FROM labels_raw

),

-- spy是标普500指数，相当于benchmark
spy_labels AS (

    SELECT
        date,
        fwd_ret_20d AS spy_fwd_ret_20d
    FROM labels
    WHERE ticker = 'SPY'

),

panel AS (

    SELECT
        f.*,

        l.fwd_ret_1d,
        l.fwd_ret_5d,
        l.fwd_ret_20d,
        l.label_direction_5d,
        l.label_direction_20d,

        l.fwd_ret_20d - s.spy_fwd_ret_20d AS fwd_excess_ret_20d_vs_spy --相对于标普500的额外收益

    FROM features AS f

    LEFT JOIN labels AS l
        ON f.security_id = l.security_id
        AND f.date = l.date

    LEFT JOIN spy_labels AS s
        ON f.date = s.date

)

SELECT *
FROM panel