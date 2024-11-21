SELECT 
    * EXCLUDE (ASSET_HKEY, ASSET_HDIFF, 
    SOURCE, LOAD_TS_UTC)
FROM {{ ref("transformed_ohlc_data") }}