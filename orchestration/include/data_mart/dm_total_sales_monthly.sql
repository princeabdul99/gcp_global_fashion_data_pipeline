

SELECT
    DATE_TRUNC(transaction_date, MONTH) AS transaction_date,
    ROUND(SUM(line_total), 2) AS total_amount
FROM `{{ params.gold_fact_table }}` 
GROUP BY transaction_date
ORDER BY transaction_date;