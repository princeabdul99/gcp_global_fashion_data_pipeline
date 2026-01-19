SELECT
  ROUND(MAX(unit_price), 2) AS highest_price,
  p.sub_category,
  p.category
FROM `ecom-pipeline-gcp.gold.fact_transactions` t
JOIN `ecom-pipeline-gcp.gold.dim_products` p 
  ON t.product_id = p.product_id
GROUP BY p.sub_category, p.category
ORDER BY highest_price DESC
LIMIT 5;  