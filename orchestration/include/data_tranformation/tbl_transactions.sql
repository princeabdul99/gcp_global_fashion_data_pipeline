
with products AS (
    SELECT
        product_id,
        category,
        sub_category
    FROM `{{ params.bronze_table_join }}`
)


SELECT
    t.invoice_id,
    t.line,
    t.customer_id,
    t.product_id,
    COALESCE(t.size, 'N/A') AS size, 
    COALESCE(t.color, 'N/A') AS color,   
    t.unit_price,
    t.quantity,
    t.date,
    t.discount,
    t.line_total,
    t.store_id,
    t.employee_id,
    t.currency,
    t.currency_symbol,
    CONCAT(
        LEFT(UPPER(COALESCE(p.category, 'XX')), 2),
        LEFT(UPPER(COALESCE(p.sub_category, 'XX')), 2),
        t.product_id
    ) AS sku,    
    t.transaction_type,
    t.payment_method,
    t.invoice_total,
    _FILE_NAME AS source_file,
    CURRENT_TIMESTAMP() AS load_time    


FROM `{{ params.bronze_table }}` t
INNER JOIN products p
    ON t.product_id = p.product_id;


-- with transactionsdata as (
--     SELECT *,
--         ROW_NUMBER() OVER(PARTITION BY invoice_id, product_id, customer_id, line ORDER BY date, line DESC) as rn
--     FROM `ecom-pipeline-gcp.silver.transactions`
-- )    
