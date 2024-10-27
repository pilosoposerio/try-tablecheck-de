{{
    config(
        materialized='table'
    )
}}

SELECT 
    r.restaurant_name
    , c.first_name
    , COUNT(t.transaction_id) as visit_count
    , SUM(t.food_cost) as total_spent
FROM {{ ref('fct__tablecheck_transactions')}} t
LEFT JOIN {{ ref('dim__tablecheck_restaurants')}} r
    ON t.restaurant_id = r.id
LEFT JOIN {{ ref('dim__tablecheck_customers')}} c
    ON t.customer_id = c.id
GROUP BY r.restaurant_name, c.first_name
ORDER BY visit_count DESC, total_spent DESC