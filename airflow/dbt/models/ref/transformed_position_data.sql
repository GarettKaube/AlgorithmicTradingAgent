{{ config(materialize="table") }}

-- Get date_id's for their respective raw date 
with transform1 as (
    {{ date_to_date_code(ref("STAGE_POSITIONS"), ref("STAGE_DATES")) }}
),

-- Calculate the current position value and the daily percent change.
transform2 as (
    SELECT 
          s_p.POSITION_HKEY
	    , s_p.POSITION_HDIFF
        , s_p.DATE_CODE
        , s_p."TIME"
        , s_p.SYMBOL_CODE
        , s_p.CURRENCY_CODE
        , s_p.POSITION_SIZE
        , s_p.TRADE_OPEN_SPREAD
        , s_p.ACCOUNT_ID
        , s_p.POSITION_SIZE * t_ohlc.CLOSE as POSITION_VALUE
        , lag(POSITION_VALUE) OVER(PARTITION BY s_p.SYMBOL_CODE ORDER by s_p.DATE_CODE) as PREVIOUS_POSITION_VAL
        , (POSITION_VALUE / PREVIOUS_POSITION_VAL) - 1 AS POSITION_VALUE_DAILY_PCT_CHANGE

    FROM transform1 as s_p
    -- Join the transformed_ohlc data to get close prices
    LEFT JOIN {{ ref("transformed_ohlc_data") }} as t_ohlc
        ON t_ohlc.SYMBOL_CODE = s_p.SYMBOL_CODE
            AND t_ohlc.DATE_CODE = s_p.DATE_CODE     
)


SELECT * FROM transform2