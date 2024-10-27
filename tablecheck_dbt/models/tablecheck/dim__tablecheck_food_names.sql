{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}
WITH new_food_names AS (
    SELECT
        food_name
        , MIN(loaded_at) AS loaded_at
    FROM {{ ref('stg__tablecheck_data')}}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
    GROUP BY food_name
),
add_id AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['food_name']) }} AS id
        , *
    FROM new_food_names
)
SELECT * FROM add_id
ORDER BY food_name