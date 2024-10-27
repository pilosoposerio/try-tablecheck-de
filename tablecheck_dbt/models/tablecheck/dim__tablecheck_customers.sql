{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}
WITH new_customers AS (
    SELECT
        first_name
        , MIN(loaded_at) AS loaded_at
    FROM {{ ref('stg__tablecheck_data')}}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
    GROUP BY first_name
),
add_id AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['first_name']) }} AS id
        , *
    FROM new_customers
)
SELECT * FROM add_id
ORDER BY first_name