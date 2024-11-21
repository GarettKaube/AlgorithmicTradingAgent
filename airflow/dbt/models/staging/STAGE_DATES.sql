{{ config(materialize="ephemeral") }}

WITH src_data as (
    SELECT
        DATE_ID AS DATE_CODE,       -- NUMBER
        "YEAR" AS "YEAR",           -- NUMBER
        "MONTH" AS "MONTH",         -- NUMBER
        "DAY" AS "DAY",             -- NUMBER
        LOAD_TS AS LOAD_TS,         -- TIMESTAMP
    FROM {{ source('seeds', 'dates') }}
),

hashed as (
    SELECT
       {{ dbt_utils.generate_surrogate_key(['DATE_CODE']) }} as DATES_HKEY,
       {{ dbt_utils.generate_surrogate_key(
                ['YEAR', 'MONTH', 'DAY']
            ) 
        }}
         as DATES_HDIFF,
        * EXCLUDE LOAD_TS,
        LOAD_TS AS LOAD_TS_UTC
    FROM src_data
)

SELECT * FROM hashed
