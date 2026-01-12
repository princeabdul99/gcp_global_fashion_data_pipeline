
SELECT
    product_id,
    category,
    sub_category,
    description_en,
    COALESCE(color, 'N/A') AS color,
    COALESCE(sizes, 'N/A') AS sizes,   
    production_cost

FROM `{{ params.bronze_table }}`