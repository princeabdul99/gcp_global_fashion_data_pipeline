SELECT 
  COUNT(invoice_id) AS total_transactions_processed,
  e.name, e.position
FROM `ecom-pipeline-gcp.silver.transactions` t
JOIN `ecom-pipeline-gcp.gold.dim_employees` e 
  ON t.employee_id = e.employee_id
WHERE e.country = 'United States'
GROUP BY e.position, e.name
ORDER BY total_transactions_processed DESC
LIMIT 10;