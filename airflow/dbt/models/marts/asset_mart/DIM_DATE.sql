SELECT * EXCLUDE (DATES_HKEY, DATES_HDIFF, LOAD_TS_UTC)
FROM {{ ref("STAGE_DATES") }}