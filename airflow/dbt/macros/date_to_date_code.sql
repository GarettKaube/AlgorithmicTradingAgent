{# Gets date_id's for their respective raw date #}
{% macro date_to_date_code(data, dates_table) %}  
    SELECT
          s_p.*
        , s_d.DATE_CODE          as DATE_CODE
    FROM {{ data }} as s_p
    JOIN {{ dates_table }} as s_d ON
        YEAR(s_p."DATE") = s_d."YEAR" AND
        MONTH(s_p."DATE") = s_d."MONTH" AND
        DAY(s_p."DATE") = s_d."DAY"
{% endmacro %}