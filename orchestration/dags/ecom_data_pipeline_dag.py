import json
import os
from datetime import datetime
from dotenv import load_dotenv

from airflow.sdk import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

load_dotenv()


args = {'owner': 'abdul-dev'}




def create_sql_ext_tbl(sql_tbl_path):
    with open(sql_tbl_path, "r") as file:
        create_ext_tbl_sql = file.read()

    return create_ext_tbl_sql


def read_json_schema(gcs_file_path):
    with open(gcs_file_path, "r") as file:
        schema_json = json.load(file)
    
    return schema_json    


# Environment Variables
GCP_PROJECT_ID = os.getenv('PROJECT_ID')

# Airflow Variables
settings = Variable.get("ecom_dag_settings", deserialize_json=True)

# DAG Variables
gcs_source_data_bucket = settings['gcs_source_data_bucket']
bq_bronze_dataset = settings['bq_bronze_dataset']
bq_silver_dataset = settings['bq_silver_dataset']
bq_gold_dataset = settings['bq_gold_dataset']

# Macros
logical_date = '{{ ds }}'

# Ext Tbl
bq_bronze_ext_table = create_sql_ext_tbl("/opt/airflow/include/raw_data_generation/bronze_ext_tbl.sql")

# ===== Stores  =====
gcs_stores_source_object = "stores/stores.csv"
gcs_stores_source_url = f"gs://{gcs_source_data_bucket}/{gcs_stores_source_object}"

bq_stores_table_name = "stores"
## Bronze - Store
bq_stores_bronze_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_stores_table_name}"
### Silver - Store
bq_stores_silver_table_id = f"{GCP_PROJECT_ID}.{bq_silver_dataset}.{bq_stores_table_name}"
bq_stores_silver_source_tbl = create_sql_ext_tbl("/opt/airflow/include/data_tranformation/tbl_stores.sql")


# ==== EMPLOYEES =====
gcs_employees_source_object = "employees/employees.csv"
gcs_employees_source_url = f"gs://{gcs_source_data_bucket}/{gcs_employees_source_object}"

bq_employees_table_name = "employees"
## Bronze  
bq_employees_bronze_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_employees_table_name}"
### Silver  
bq_employees_silver_table_id = f"{GCP_PROJECT_ID}.{bq_silver_dataset}.{bq_employees_table_name}"
bq_employees_silver_source_tbl = create_sql_ext_tbl("/opt/airflow/include/data_tranformation/tbl_employees.sql")


# ==== PRODUCTS =====
gcs_products_source_object = "products/products.csv"
gcs_products_source_url = f"gs://{gcs_source_data_bucket}/{gcs_products_source_object}"

bq_products_table_name = "products"
## Bronze  
bq_products_bronze_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_products_table_name}"
### Silver  
bq_products_silver_table_id = f"{GCP_PROJECT_ID}.{bq_silver_dataset}.{bq_products_table_name}"
bq_products_silver_source_tbl = create_sql_ext_tbl("/opt/airflow/include/data_tranformation/tbl_products.sql")


# ==== CUSTOMERS =====
gcs_customers_source_object = "customers/customers-*.csv"
gcs_customers_source_url = f"gs://{gcs_source_data_bucket}/{gcs_customers_source_object}"

bq_customers_table_name = "customers"
## Bronze  
bq_customers_bronze_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_customers_table_name}"
### Silver  
bq_customers_silver_table_id = f"{GCP_PROJECT_ID}.{bq_silver_dataset}.{bq_customers_table_name}"
bq_customers_silver_source_tbl = create_sql_ext_tbl("/opt/airflow/include/data_tranformation/tbl_customers.sql")


# ==== TRANSACTIONS =====
gcs_transactions_source_object = "transactions/transaction-*.csv.gz"
gcs_transactions_source_url = f"gs://{gcs_source_data_bucket}/{gcs_transactions_source_object}"

bq_transactions_table_name = "transactions"
## Bronze  
bq_transactions_bronze_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_transactions_table_name}"
### Silver  
bq_transactions_silver_table_id = f"{GCP_PROJECT_ID}.{bq_silver_dataset}.{bq_transactions_table_name}"
bq_transactions_silver_source_tbl = create_sql_ext_tbl("/opt/airflow/include/data_tranformation/tbl_transactions.sql")


# DWH - Gold Layer
bq_fact_sales_table_name = "fact_transaction_sales"
bq_fact_sales_table_id = f"{GCP_PROJECT_ID}.{bq_gold_dataset}.{bq_fact_sales_table_name}"

bq_dim_customers_table_name = "dim_customers"
bq_dim_customers_table_id = f"{GCP_PROJECT_ID}.{bq_gold_dataset}.{bq_dim_customers_table_name}"

bq_dim_products_table_name = "dim_products"
bq_dim_products_table_id = f"{GCP_PROJECT_ID}.{bq_gold_dataset}.{bq_dim_products_table_name}"

bq_dim_employees_table_name = "dim_employees"
bq_dim_employees_table_id = f"{GCP_PROJECT_ID}.{bq_gold_dataset}.{bq_dim_employees_table_name}"

bq_dim_stores_table_name = "dim_stores"
bq_dim_stores_table_id = f"{GCP_PROJECT_ID}.{bq_gold_dataset}.{bq_dim_stores_table_name}"






with DAG(
    dag_id = 'dag_data_load_bigquery',
    default_args=args,
    schedule='0 5 * * *',
    start_date=datetime(2026, 1, 1),

) as dag:
    ### Load Bronze Stores Table  ###
    gcs_to_bq_bronze_stores_external_tbl = BigQueryInsertJobOperator(
        task_id="gcs_to_bq_bronze_stores_external_tbl",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": bq_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_stores_table_name,
            "gcs_uri": gcs_stores_source_url,
            "skip_leading_rows": 1,
        },        
        
    )
   
    ### Load SILVER Stores Tables  ###
    bq_to_bq_silver_stores_tbl = BigQueryInsertJobOperator(
        task_id="bq_to_bq_silver_stores_tbl",
        project_id=GCP_PROJECT_ID,
        location="US",
        configuration={
            "query": {
                "query": bq_stores_silver_source_tbl,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": bq_silver_dataset,
                    "tableId": bq_stores_table_name
                },
                "createDisposition": 'CREATE_IF_NEEDED',
                "writeDisposition": 'WRITE_TRUNCATE',
                "useLegacySql": False,
                "priority": 'BATCH'                
            }
        },
        params={
            "bronze_table": bq_stores_bronze_table_id,
        },        
    ) 

    ### Load Bronze Employees Table  ###
    gcs_to_bq_bronze_empployee_external_tbl = BigQueryInsertJobOperator(
        task_id="gcs_to_bq_bronze_empployee_external_tbl",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": bq_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_employees_table_name,
            "gcs_uri": gcs_employees_source_url,
            "skip_leading_rows": 1,
        },        
        
    )

    ### Load SILVER Employees Tables  ###
    bq_to_bq_silver_emp_tbl = BigQueryInsertJobOperator(
        task_id="bq_to_bq_silver_emp_tbl",
        project_id=GCP_PROJECT_ID,
        location="US",
        configuration={
            "query": {
                "query": bq_employees_silver_source_tbl,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": bq_silver_dataset,
                    "tableId": bq_employees_table_name
                },
                "createDisposition": 'CREATE_IF_NEEDED',
                "writeDisposition": 'WRITE_TRUNCATE',
                "useLegacySql": False,
                "priority": 'BATCH'                
            }
        },
        params={
            "bronze_table": bq_employees_bronze_table_id,
        },        
    ) 

    ### Load Bronze Products Table  ###
    gcs_to_bq_bronze_product_external_tbl = BigQueryInsertJobOperator(
        task_id="gcs_to_bq_bronze_product_external_tbl",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": bq_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_products_table_name,
            "gcs_uri": gcs_products_source_url,
            "skip_leading_rows": 1,
        },        
        
    )

    ### Load SILVER Products Tables  ###
    bq_to_bq_silver_product_tbl = BigQueryInsertJobOperator(
        task_id="bq_to_bq_silver_product_tbl",
        project_id=GCP_PROJECT_ID,
        location="US",
        configuration={
            "query": {
                "query": bq_products_silver_source_tbl,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": bq_silver_dataset,
                    "tableId": bq_products_table_name
                },
                "createDisposition": 'CREATE_IF_NEEDED',
                "writeDisposition": 'WRITE_TRUNCATE',
                "useLegacySql": False,
                "priority": 'BATCH'                
            }
        },
        params={
            "bronze_table": bq_products_bronze_table_id,
        },        
    ) 

    ### Load Bronze Customers Table  ###
    gcs_to_bq_bronze_customer_external_tbl = BigQueryInsertJobOperator(
        task_id="gcs_to_bq_bronze_customer_external_tbl",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": bq_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_customers_table_name,
            "gcs_uri": gcs_customers_source_url,
            "skip_leading_rows": 1,
        },        
        
    )

    ### Load SILVER Customers Tables  ###
    bq_to_bq_silver_customer_tbl = BigQueryInsertJobOperator(
        task_id="bq_to_bq_silver_customer_tbl",
        project_id=GCP_PROJECT_ID,
        location="US",
        configuration={
            "query": {
                "query": bq_customers_silver_source_tbl,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": bq_silver_dataset,
                    "tableId": bq_customers_table_name
                },
                "createDisposition": 'CREATE_IF_NEEDED',
                "writeDisposition": 'WRITE_TRUNCATE',
                "useLegacySql": False,
                "priority": 'BATCH'                
            }
        },
        params={
            "bronze_table": bq_customers_bronze_table_id,
        },        
    ) 

    ### Load Bronze transactions Table  ###
    gcs_to_bq_bronze_transaction_external_tbl = BigQueryInsertJobOperator(
        task_id="gcs_to_bq_bronze_transaction_external_tbl",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": bq_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_transactions_table_name,
            "gcs_uri": gcs_transactions_source_url,
            "skip_leading_rows": 1,
        },        
        
    )

    ### Load SILVER transactions Tables  ###
    bq_to_bq_silver_transaction_tbl = BigQueryInsertJobOperator(
        task_id="bq_to_bq_silver_transaction_tbl",
        project_id=GCP_PROJECT_ID,
        location="US",
        configuration={
            "query": {
                "query": bq_transactions_silver_source_tbl,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": bq_silver_dataset,
                    "tableId": bq_transactions_table_name
                },
                "createDisposition": 'CREATE_IF_NEEDED',
                "writeDisposition": 'WRITE_TRUNCATE',
                "useLegacySql": False,
                "priority": 'BATCH'                
            }
        },
        params={
            "bronze_table": bq_transactions_bronze_table_id,
            "bronze_table_join": bq_products_bronze_table_id
        },        
    ) 






    gcs_to_bq_bronze_stores_external_tbl >> bq_to_bq_silver_stores_tbl
    gcs_to_bq_bronze_empployee_external_tbl >> bq_to_bq_silver_emp_tbl
    gcs_to_bq_bronze_product_external_tbl >> bq_to_bq_silver_product_tbl
    gcs_to_bq_bronze_customer_external_tbl >> bq_to_bq_silver_customer_tbl
    gcs_to_bq_bronze_transaction_external_tbl >> bq_to_bq_silver_transaction_tbl

if __name__ == "__main__":
    dag.cli()
