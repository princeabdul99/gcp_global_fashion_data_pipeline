
with stores AS (
    SELECT
        store_id,
        city,
        country
    FROM `{{ params.silver_table_join }}`
)

SELECT 
    employee_id,
    name,
    position,
    s.city,
    s.country
FROM `{{ params.silver_table }}` e
INNER JOIN stores s
    ON e.store_id = s.store_id