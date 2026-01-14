SELECT
    product_id,
    category,
    sub_category,
    description_en,
    color,
    sizes,   
    production_cost

FROM `{{ params.silver_table }}`