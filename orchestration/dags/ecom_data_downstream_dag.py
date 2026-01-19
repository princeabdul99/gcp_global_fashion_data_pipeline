import os
from datetime import datetime
from dotenv import load_dotenv

from airflow.sdk import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator

load_dotenv()


args = {'owner': 'abdul-dev'}

def create_sql_ext_tbl(sql_tbl_path):
    with open(sql_tbl_path, "r") as file:
        create_ext_tbl_sql = file.read()

    return create_ext_tbl_sql

# Environment Variables
GCP_PROJECT_ID = os.getenv('PROJECT_ID')

# Airflow Variables
settings = Variable.get("ecom_dag_settings", deserialize_json=True)

# DAG Variables
gcs_source_data_bucket = settings['gcs_source_data_bucket']
bq_gold_dataset = settings['bq_gold_dataset']

# FACT - Gold Layer
bq_fact_sales_table_id = f"{GCP_PROJECT_ID}.{bq_gold_dataset}.fact_transactions"

# DATA-MART - Gold Layer
dm_total_daily_sales_table_name = "dm_sum_total_sale_daily"
dm_total_daily_sales_table_id = f"{GCP_PROJECT_ID}.{bq_gold_dataset}.{dm_total_daily_sales_table_name}"
dm_total_daily_sales_source_tbl = create_sql_ext_tbl("/opt/airflow/include/data_mart/dm_total_sales_daily.sql")


dm_total_monthly_sales_table_name = "dm_sum_total_sale_month"
dm_total_monthly_sales_table_id = f"{GCP_PROJECT_ID}.{bq_gold_dataset}.{dm_total_monthly_sales_table_name}"
dm_total_monthly_sales_source_tbl = create_sql_ext_tbl("/opt/airflow/include/data_mart/dm_total_sales_monthly.sql")


with DAG(
    dag_id='ecom_data_downstream_dag',
    default_args=args,
    schedule=[
        Dataset('gold_fact_transactions'),
        Dataset('gold_dim_stores'),
        Dataset('gold_dim_employees'),
        Dataset('gold_dim_products'),
        Dataset('gold_dim_customers')
    ],
    start_date=datetime(2026, 1, 1)
) as dag:

    dm_daily_total_sales = BigQueryInsertJobOperator(
        task_id = "dm_daily_total_sales",
        configuration={
            "query": {
                "query": dm_total_daily_sales_source_tbl,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId":  bq_gold_dataset,
                    "tableId": dm_total_daily_sales_table_name
                },
                # "timePartitioning": {
                #     'type': 'DAY',
                #     'field': 'transaction_date'
                # },
                "createDisposition": 'CREATE_IF_NEEDED',
                "writeDisposition": 'WRITE_TRUNCATE',
                "priority": "BATCH",
            },            
        },
        params={
            "gold_fact_table": bq_fact_sales_table_id
        },        
    )


    dm_monthly_total_sales = BigQueryInsertJobOperator(
        task_id = "dm_month_total_sales",
        configuration={
            "query": {
                "query": dm_total_monthly_sales_source_tbl,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId":  bq_gold_dataset,
                    "tableId": dm_total_monthly_sales_table_name
                },
                "createDisposition": 'CREATE_IF_NEEDED',
                "writeDisposition": 'WRITE_TRUNCATE',
                "priority": "BATCH",
            },            
        },
        params={
            "gold_fact_table": bq_fact_sales_table_id
        },        
    )    


    ######## BQ Row Count Checker 
    bq_row_count_check_data_mart_sum_total_sales_daily = BigQueryCheckOperator(
        task_id='bq_row_count_check_data_mart_sum_total_sales_daily',
        sql=f"""
        select count(*) from `{dm_total_daily_sales_table_id}`
        """,
        use_legacy_sql=False,
        outlets=[Dataset("dm_sum_daily_total_sales")]
    )        

    bq_row_count_check_data_mart_sum_total_sales_monthly = BigQueryCheckOperator(
        task_id='bq_row_count_check_data_mart_sum_total_sales_monthly',
        sql=f"""
        select count(*) from `{dm_total_daily_sales_table_id}`
        """,
        use_legacy_sql=False,
        outlets=[Dataset("dm_sum_monthly_total_sales")]
    ) 

    dm_daily_total_sales >> bq_row_count_check_data_mart_sum_total_sales_daily
    dm_monthly_total_sales >> bq_row_count_check_data_mart_sum_total_sales_monthly

if __name__ == "__main__":
    dag.cli()    