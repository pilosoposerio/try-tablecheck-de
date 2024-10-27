{{
    config(
        materialized='table'
    )
}}

SELECT 
    r.restaurant_name
    , f.food_name
    , COUNT(t.transaction_id) as total_orders
    , SUM(t.food_cost) as total_revenue
FROM {{ ref('fct__tablecheck_transactions')}} t
LEFT JOIN {{ ref('dim__tablecheck_restaurants')}} r
    ON t.restaurant_id = r.id
LEFT JOIN {{ ref('dim__tablecheck_food_names')}} f
    ON t.food_id = f.id
GROUP BY r.restaurant_name, f.food_name
ORDER BY total_revenue DESC, total_orders DESC