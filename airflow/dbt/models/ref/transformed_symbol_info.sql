WITH current_snapshot as (
    {{ snapshot_selection(ref('SNSH_SYMBOL_INFO')) }}
)

SELECT *
FROM current_snapshot