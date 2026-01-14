DELETE FROM `{{ params.gold_table }}`
WHERE transaction_date IN (
    SELECT DISTINCT DATE(date)
    FROM `{{ params.silver_table }}`
);


INSERT INTO `{{ params.gold_table }}`
with products AS (
    SELECT
        product_id,
        category,
        sub_category
    FROM `{{ params.dim_table_join_product }}`
),
employees AS (
    SELECT
        employee_id,
        name,
        position,
    FROM `{{ params.dim_table_join_emp }}`
),
stores AS (
    SELECT
        store_id,
        city,
        country
    FROM `{{ params.dim_table_join_store }}`
)

SELECT
    invoice_id,
    line,
    customer_id,
    -- p.product_id,
    p.category AS category,
    size, 
    color,   
    unit_price,
    quantity,
    DATE(date) AS transaction_date,
    discount,
    line_total,
    s.city,
    s.country,
    e.position AS processed_by,
    currency,
    currency_symbol,
    sku,    
    transaction_type,
    payment_method,
    invoice_total


FROM `{{ params.silver_table }}` t
INNER JOIN products p
    ON t.product_id = p.product_id
INNER JOIN employees e
    ON t.employee_id = e.employee_id
INNER JOIN stores s
    ON t.store_id = s.store_id;
