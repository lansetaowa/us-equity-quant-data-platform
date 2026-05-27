WITH source AS (

    SELECT
        CAST(security_id AS VARCHAR) AS security_id,
        CAST(ticker AS VARCHAR) AS ticker,
        CAST(date AS DATE) AS date,

        CAST(open AS DOUBLE) AS open,
        CAST(high AS DOUBLE) AS high,
        CAST(low AS DOUBLE) AS low,
        CAST(close AS DOUBLE) AS close,
        CAST(volume AS BIGINT) AS volume,

        CAST(adj_open AS DOUBLE) AS adj_open,
        CAST(adj_high AS DOUBLE) AS adj_high,
        CAST(adj_low AS DOUBLE) AS adj_low,
        CAST(adj_close AS DOUBLE) AS adj_close,
        CAST(adj_volume AS BIGINT) AS adj_volume,

        CAST(div_cash AS DOUBLE) AS div_cash,
        CAST(split_factor AS DOUBLE) AS split_factor,

        CAST(source AS VARCHAR) AS source,
        CAST(load_id AS VARCHAR) AS load_id,
        CAST(loaded_at AS VARCHAR) AS loaded_at

    FROM read_parquet('../data/dwd/equity_price_daily/**/part-*.parquet')

)

SELECT *
FROM source