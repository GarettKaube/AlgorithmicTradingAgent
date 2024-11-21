-- Check the range of close and adjusted_close
SELECT "CLOSE", ADJUSTED_CLOSE
FROM {{ ref("transformed_ohlc_data") }}
WHERE "CLOSE" > 1000000 or ADJUSTED_CLOSE > 1000000
    or "CLOSE" < 0 OR ADJUSTED_CLOSE < 0