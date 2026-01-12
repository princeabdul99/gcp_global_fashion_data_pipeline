
SELECT 
    store_id,
    CASE 
        WHEN country = '中国' THEN 'China'
        WHEN country = 'España' THEN 'Spain'
        ELSE country
    END AS country,
    
    CASE 
        WHEN city = '上海' THEN 'Shanghai'
        WHEN city = '北京' THEN 'Beijing'
        WHEN city = '广州' THEN 'Guangzhou'
        WHEN city = '深圳' THEN 'Shenzhen'
        WHEN city = '重庆' THEN 'Chongqing'
        ELSE city
    END AS city,    
    
    CASE 
        WHEN store_name = 'Store 上海' THEN 'Store Shanghai'
        WHEN store_name = 'Store 北京' THEN 'Store Beijing'
        WHEN store_name = 'Store 广州' THEN 'Store Guangzhou'
        WHEN store_name = 'Store 深圳' THEN 'Store Shenzhen'
        WHEN store_name = 'Store 重庆' THEN 'Store Chongqing'
        ELSE store_name
    END AS store_name,
    number_of_employees,
    zip_code,
    latitude,
    longitude     
FROM `{{ params.bronze_table }}`