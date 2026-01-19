SELECT name, country, position 
FROM `ecom-pipeline-gcp.gold.dim_employees` 
WHERE position = 'Sales Associate' AND country = 'United States'