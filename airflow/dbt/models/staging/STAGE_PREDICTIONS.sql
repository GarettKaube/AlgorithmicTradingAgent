{{ config(materialized='view') }}



WITH src_data as (
    SELECT 
         "DATE"           as "DATE"              -- DATE
        , PREDICTION      AS PREDICTION          -- NUMBER
        , CONFIDENCE      AS CONFIDENCE          -- NUMBER
        , ACTUAL          AS ACTUAL              -- NUMBER
        , MODEL_ID        AS MODEL_ID            -- TEXT   
        ,'SOURCE_DATA.PREDICTION_SOURCE' as SOURCE
    FROM {{ source("asset_data_source", "PREDICTION_SOURCE") }}
),

hashed as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["DATE"]) }} as PREDICTION_HKEY,
        {{ dbt_utils.generate_surrogate_key(
                ["DATE", 'PREDICTION', 'CONFIDENCE',
                'ACTUAL', 'MODEL_ID'
                ]
            ) 
        }} as PREDICTION_HDIFF,

        *,

        '{{ run_started_at }}' as LOAD_TS_UTC -- Load timestamp
    FROM src_data
),

default_record as (
    SELECT DISTINCT
         '2000-01-01'           as "DATE"
        , 0                     as PREDICTION
        , 0                     as CONFIDENCE
        , 0                     as ACTUAL
        , 'Missing'             as MODEL_ID
        , 'System.DefaultKey'   as SOURCE
),

with_default_record as (
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
)

SELECT * FROM hashed