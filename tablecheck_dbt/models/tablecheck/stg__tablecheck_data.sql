{{
    config(
        materialized='table',
        unique_key='table_name'
    )
}}
WITH raw_data AS (
    SELECT
        TRIM(restaurant_names) as restaurant_name
        , TRIM(food_names) as food_name
        , TRIM(first_name) as first_name
        , ROUND(food_cost, 2) as food_cost
        , "filename"
        , loaded_at
        , transaction_id
    FROM {{ source('tablecheck', 'raw_data')}}
)
, remove_null_rows AS (
    SELECT
        *
    FROM raw_data
    WHERE NOT COLUMNS(* EXCLUDE(loaded_at, transaction_id)) IS NULL
)
SELECT * FROM remove_null_rows