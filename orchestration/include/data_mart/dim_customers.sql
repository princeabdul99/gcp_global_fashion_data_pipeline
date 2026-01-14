SELECT 
    customer_id,
    name,
    email,
    telephone, 
    city,      
    country,
    gender,
    date_of_birth,
    job_title
     
FROM `{{ params.silver_table }}`