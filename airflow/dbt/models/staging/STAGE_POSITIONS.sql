{{ config(materialized='table') }}


WITH src_data as (
    SELECT 
        "DATE"              as "DATE",             -- DATE
        "TIME"              as "TIME", 
        SYMBOL              as SYMBOL_CODE,         -- TEXT 
        CURRENCY            as CURRENCY_CODE,       -- TEXT 
        POSITION_SIZE       as POSITION_SIZE,       -- NUMBER 
        TRADE_OPEN_SPREAD   as TRADE_OPEN_SPREAD,
        ACCOUNT_ID          as ACCOUNT_ID,           -- TEXT 
        'SOURCE_DATA.POSITION_SOURCE' as SOURCE
    FROM {{ source("asset_data_source", "POSITION_SOURCE") }}
),

hashed as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['DATE', 'TIME', 'SYMBOL_CODE']) }} as POSITION_HKEY,
        {{ dbt_utils.generate_surrogate_key(
                ['SYMBOL_CODE', 'DATE', 'TIME', 'CURRENCY_CODE',
                'POSITION_SIZE',"TRADE_OPEN_SPREAD", "ACCOUNT_ID"
                ]
            ) 
        }} as POSITION_HDIFF,

        *,

        '{{ run_started_at }}' as LOAD_TS_UTC -- Load timestamp
    FROM src_data
),

default_record as (
    SELECT DISTINCT
          '2020-01-01'          as "DATE"
        , '00:00:00'            as "TIME"
        , 'Missing'             as SYMBOL_CODE
        , 'Missing'             as CURRENCY_CODE
        , -1                    as POSITION_SIZE
        , -1                    as TRADE_OPEN_SPREAD
        , 'Missing'             as ACCOUNT_ID
        , 'System.DefaultKey'   as SOURCE
),

with_default_record as (
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
)

SELECT * FROM hashed
