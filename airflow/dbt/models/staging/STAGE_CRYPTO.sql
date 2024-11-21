{{ config(materialized='view') }}



WITH src_data as (
    SELECT 
        "DATE"          as "DATE",              -- DATE   
        "OPEN"          as "OPEN",              -- NUMBER 
        "HIGH"          as "HIGH",              -- NUMBER 
        "CLOSE"         as "CLOSE",             -- NUMBER 
        ADJ_CLOSE       as ADJUSTED_CLOSE,      -- NUMBER 
        LOW             as LOW,                 -- NUMBER                     
        VOLUME          as VOLUME,              -- NUMBER
        SYMBOL          as SYMBOL_CODE,         -- TEXT 
        EXCHANGE        as EXCHANGE_CODE,       -- TEXT 
        CURRENCY        as CURRENCY_CODE,       -- TEXT  
        PREDICTION      as PREDICTION,          -- NUMBER
        ACTUAL          as ACTUAL,              -- NUMBER
        MODEL_ID        as MODEL_ID,             -- TEXT
        'SOURCE_DATA.ASSET_SOURCE' as SOURCE
    FROM {{ source("asset_data_source", "ASSET_SOURCE") }}
),

hashed as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["DATE", 'SYMBOL_CODE']) }} as ASSET_HKEY,
        {{ dbt_utils.generate_surrogate_key(
                ["DATE", 'SYMBOL_CODE', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
                'ADJUSTED_CLOSE', 'VOLUME', 'EXCHANGE_CODE',
                'CURRENCY_CODE', 'PREDICTION',
                'ACTUAL', 'MODEL_ID'
                ]
            ) 
        }} as ASSET_HDIFF,

        *,

        '{{ run_started_at }}' as LOAD_TS_UTC -- Load timestamp
    FROM src_data
),

default_record as (
    SELECT DISTINCT
        -1 as ID
        , '2000-01-01' as DATE
        , -1 as OPEN
        , -1 as HIGH
        , -1 as CLOSE
        , -1 as ADJUSTED_CLOSE
        , -1 as LOW
        , -1 as VOLUME
        , 'Missing' as SYMBOL
        , 'Missing' as SECURITY_NAME
        , 'Missing' as EXCHANGE
        , 'Missing' as CURRENCY
        , 'Missing' as TIMEZONE
        , 0 as PREDICTION
        , 0 as ACTUAL
        , 'Missing' as MODEL_ID
)

SELECT * FROM hashed


