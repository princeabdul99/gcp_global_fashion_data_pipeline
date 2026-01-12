SELECT 
    employee_id,
    store_id,
    name,
    position
FROM `{{ params.bronze_table }}`