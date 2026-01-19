with productsdata as (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY product_id) as rn
    FROM `{{ params.bronze_table }}`
)


SELECT
    product_id,
    category,
    sub_category,
    description_en,
    COALESCE(color, 'N/A') AS color,
    COALESCE(sizes, 'N/A') AS sizes,   
    production_cost

FROM productsdata
WHERE rn = 1