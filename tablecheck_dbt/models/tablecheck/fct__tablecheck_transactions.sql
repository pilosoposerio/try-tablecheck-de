{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}
WITH new_transactions AS (
    SELECT
        transaction_id
        , restaurant_name
        , first_name
        , food_name 
        , food_cost
        , loaded_at
    FROM {{ ref('stg__tablecheck_data')}}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),
add_id AS (
    SELECT
        transaction_id
        , {{ dbt_utils.generate_surrogate_key(['restaurant_name']) }} AS restaurant_id
        , {{ dbt_utils.generate_surrogate_key(['first_name']) }} AS customer_id
        , {{ dbt_utils.generate_surrogate_key(['food_name']) }} AS food_id
        , food_cost
        , loaded_at
    FROM new_transactions
)
SELECT * FROM add_id