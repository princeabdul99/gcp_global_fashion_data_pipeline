SELECT
  ROUND(MIN(unit_price), 2) AS lowest_price,
  p.sub_category,
  p.category
FROM `ecom-pipeline-gcp.gold.fact_transactions` t
JOIN `ecom-pipeline-gcp.gold.dim_products` p 
  ON t.product_id = p.product_id
GROUP BY p.sub_category, p.category
ORDER BY lowest_price DESC
LIMIT 5;  
