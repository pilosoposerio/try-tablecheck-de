{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}
WITH new_sources AS (
    SELECT DISTINCT
        "filename"
        , loaded_at
    FROM {{ ref('stg__tablecheck_data')}}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),
add_id AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['filename', 'loaded_at']) }} AS id
        , *
    FROM new_sources
)
SELECT * FROM add_id