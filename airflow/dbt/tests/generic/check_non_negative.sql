{% test check_non_negative(model, column_name) %}
WITH column_data as (
    SELECT {{column_name}} as column_ FROM {{model}}
),
validation_errors as (
    SELECT column_
    FROM column_data 
    WHERE column_ < 0
)

SELECT * FROM validation_errors
{% endtest %}