CREATE TABLE IF NOT EXISTS `{{ params.gold_table }}`
(
    invoice_id STRING,
    line INT64,
    customer_id INT64,
    product_id INT64,
    size STRING,
    color STRING,
    unit_price FLOAT64,
    quantity INT64,
    transaction_date DATE,
    discount FLOAT64,
    line_total FLOAT64,
    store_id INT64,
    employee_id INT64,
    currency STRING,
    currency_symbol STRING,
    sku STRING,
    transaction_type STRING,
    payment_method STRING,
    invoice_total FLOAT64
) PARTITION BY transaction_date;

DELETE FROM `{{ params.gold_table }}`
WHERE transaction_date IN (
    SELECT DISTINCT DATE(date)
    FROM `{{ params.silver_table }}`
);


INSERT INTO `{{ params.gold_table }}`
with products AS (
    SELECT
        product_id,
    FROM `{{ params.dim_table_join_product }}`
),
employees AS (
    SELECT
        employee_id,
    FROM `{{ params.dim_table_join_emp }}`
),
stores AS (
    SELECT
        store_id,
    FROM `{{ params.dim_table_join_store }}`
),
customers AS (
    SELECT
        customer_id,
    FROM `{{ params.dim_table_join_customer }}`
)

SELECT
    invoice_id,
    line,
    c.customer_id,
    p.product_id,
    size, 
    color,   
    unit_price,
    quantity,
    DATE(date) AS transaction_date,
    discount,
    line_total,
    s.store_id,
    e.employee_id,
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
    ON t.store_id = s.store_id
INNER JOIN customers c
    ON t.customer_id = c.customer_id;    
