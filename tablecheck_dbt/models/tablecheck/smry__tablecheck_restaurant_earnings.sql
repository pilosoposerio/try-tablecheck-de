{{
    config(
        materialized='table'
    )
}}

SELECT 
    r.restaurant_name,
    SUM(t.food_cost) as earnings
FROM {{ ref('fct__tablecheck_transactions')}} t
LEFT JOIN {{ ref('dim__tablecheck_restaurants')}} r
    ON t.restaurant_id = r.id
GROUP BY r.restaurant_name