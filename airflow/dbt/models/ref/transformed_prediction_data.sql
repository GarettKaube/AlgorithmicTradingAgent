{{ config(materialize="table") }}

-- Get date_id's for their respective raw date 
with transform1 as (
    {{ date_to_date_code(ref("STAGE_PREDICTIONS"), ref("STAGE_DATES")) }}
)

SELECT * FROM transform1
