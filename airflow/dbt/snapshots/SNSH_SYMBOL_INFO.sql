{% snapshot SNSH_SYMBOL_INFO %}
{{
    config(
        unique_key='SYMBOL_HKEY',
        strategy='check',
        check_cols=['SYMBOL_HDIFF'],
    )
}}
SELECT * FROM {{ ref('STAGE_SYMBOL_INFO') }}
{% endsnapshot %}