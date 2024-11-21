{{ config(materialize="table") }}

-- add date codes
with transform1 as (
    {{ date_to_date_code(ref("STAGE_CRYPTO"), ref("STAGE_DATES")) }}
),

-- Calculate Close direction and moving average of dollar volume
transform2 as (
    SELECT *,
        "CLOSE" * VOLUME as DOLLAR_VOLUME,
        "CLOSE" - LAG("CLOSE") 
            OVER(PARTITION BY SYMBOL_CODE ORDER BY "DATE") as CLOSE_CHANGE,
        SIGN(CLOSE_CHANGE) as CLOSE_DIRECTION,
        AVG("CLOSE") 
            OVER(
                PARTITION BY SYMBOL_CODE 
                ORDER BY "DATE" 
                ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) 
            AS DOLLAR_VOLUME1M,
    FROM transform1
),

-- calculate True Range
TR as (
    SELECT
        *,
        HIGH - LOW AS "RANGE",
        ABS(HIGH - LAG("CLOSE") OVER(PARTITION BY SYMBOL_CODE ORDER BY "DATE")) as HIGH_AND_PREV_CLOSE_DIST,
        ABS(LOW - LAG("CLOSE") OVER(PARTITION BY SYMBOL_CODE ORDER BY "DATE")) as LOW_AND_PREV_CLOSE_DIST,
        GREATEST("RANGE", HIGH_AND_PREV_CLOSE_DIST, LOW_AND_PREV_CLOSE_DIST) AS TRUE_RANGE,
        
    FROM transform2
),

transform3 as (
    SELECT 
        * EXCLUDE(HIGH_AND_PREV_CLOSE_DIST, LOW_AND_PREV_CLOSE_DIST)
    FROM TR
),

-- calculate vwap
vwap as (
    SELECT
        *,
        (1/3)*(HIGH + LOW + "CLOSE") AS HLC3,
        HLC3*VOLUME as HLC3_DOLLAR_VOLUME,
        SUM(HLC3_DOLLAR_VOLUME) OVER(PARTITION BY SYMBOL_CODE ORDER BY "DATE") as CUMULATIVE_DOLLAR_VOLUME,
        SUM(VOLUME) OVER(PARTITION BY SYMBOL_CODE ORDER BY "DATE") as CUMULATIVE_VOLUME,
        CUMULATIVE_DOLLAR_VOLUME / CUMULATIVE_VOLUME AS VWAP,
    FROM transform3
),

transform4 as (
    SELECT * EXCLUDE(CUMULATIVE_DOLLAR_VOLUME, CUMULATIVE_VOLUME)
    FROM vwap
)


SELECT * EXCLUDE (HLC3, HLC3_DOLLAR_VOLUME, "DATE")
FROM transform4
