with employeesdata as (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY employee_id ORDER BY employee_id) as rn
    FROM `{{ params.bronze_table }}`
)

SELECT 
    employee_id,
    store_id,
    name,
    position
FROM employeesdata
WHERE rn = 1