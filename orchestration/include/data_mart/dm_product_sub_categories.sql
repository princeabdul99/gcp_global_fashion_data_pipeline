SELECT 
  sub_category
FROM `ecom-pipeline-gcp.gold.dim_products` 
GROUP BY sub_category;
