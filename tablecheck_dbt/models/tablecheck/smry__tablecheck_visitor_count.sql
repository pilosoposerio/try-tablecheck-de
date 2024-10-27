{{
    config(
        materialized='table'
    )
}}

SELECT 
    r.restaurant_name
    , COUNT(DISTINCT customer_id) as unique_visitors_count
    , COUNT(customer_id) as visitors_count
FROM {{ ref('fct__tablecheck_transactions')}} t
LEFT JOIN {{ ref('dim__tablecheck_restaurants')}} r
    ON t.restaurant_id = r.id
GROUP BY r.restaurant_name