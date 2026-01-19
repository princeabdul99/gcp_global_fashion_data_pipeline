-- The total amount of products in each category. Sort the results by the total number of products in descendind order.
SELECT 
  COUNT(*) AS total_count,
  category,
  sub_category
FROM `ecom-pipeline-gcp.gold.dim_products`
GROUP BY category, sub_category
ORDER BY category;