
SELECT 
    store_id,
    country,
    city,    
    store_name,
    number_of_employees,
    zip_code,
    latitude,
    longitude     
FROM `{{ params.silver_table }}`