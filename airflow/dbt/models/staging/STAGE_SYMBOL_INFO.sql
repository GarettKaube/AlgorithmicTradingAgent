{{ config(materialized="ephemeral") }}

WITH src_data as (
    SELECT
        SYMBOL AS SYMBOL_CODE,          -- TEXT
        EXCHANGE AS EXCHANGE_CODE,      -- TEXT
        NAME AS SECURITY_NAME,          -- TEXT
        SECTOR AS SECTOR,               -- TEXT
        INDUSTRY AS INDUSTRY,           -- TEXT
        ASSET_TYPE AS ASSET_TYPE,       -- TEXT
        LOAD_TS AS LOAD_TS,             -- TIMESTAMP
    FROM {{ source('seeds', 'symbol_info') }}
),

hashed as (
    SELECT
       {{ dbt_utils.generate_surrogate_key(['SYMBOL_CODE']) }} as SYMBOL_HKEY,
       {{ dbt_utils.generate_surrogate_key(
                ['SYMBOL_CODE', 'EXCHANGE_CODE', 'SECURITY_NAME',
                'SECTOR', 'INDUSTRY', 'ASSET_TYPE'
                ]
            ) 
        }}
         as SYMBOL_HDIFF,
        * EXCLUDE LOAD_TS,
        LOAD_TS AS LOAD_TS_UTC
    FROM src_data
),


default_record as (
    SELECT DISTINCT
        -1 as SYMBOL_CODE
        , -1 as EXCHANGE_CODE
        , 'Missing' as SECURITY_NAME
        , 'Missing' as SECTOR
        , 'Missing' as INDUSTRY
        , 'Missing' as ASSET_TYPE
        , '2000-01-01' as LOAD_TS_UTC
        , 'System.DefaultKey' as RECORD_SOURCE
)

-- with_default_record as (
--     SELECT * FROM src_data
--     UNION ALL
--     SELECT * FROM default_record
-- )

SELECT * FROM hashed

