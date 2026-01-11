import os
from airflow.sdk import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator 
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()



GCP_PROJECT_ID = os.getenv('PROJECT_ID')

args = {
    'owner': 'abdul-dev'
}


with DAG(
    dag_id = 'dag_load_bigquery',
    default_args=args,
    schedule='0 5 * * *',
    start_date=datetime(2026, 1, 1),

) as dag:
    gcs_to_bq_stores = GCSToBigQueryOperator(
        task_id = "gcs_to_bq_stores",
        bucket='{}-bucket'.format(GCP_PROJECT_ID),
        external_table=True,
        source_objects=['stores/stores.csv'],
        destination_project_dataset_table='bronze.stores',
        schema_fields = [
            {'name': 'store_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'store_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'number_of_employees', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'zip_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'latitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'longitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE'
    )


    gcs_to_bq_employees = GCSToBigQueryOperator(
        task_id = "gcs_to_bq_employees",
        bucket='{}-bucket'.format(GCP_PROJECT_ID),
        external_table=True,
        source_objects=['employees/employees.csv'],
        destination_project_dataset_table='bronze.employees',
        schema_fields = [
            {'name': 'employeeid', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'storeid', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'position', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE'
    )

    gcs_to_bq_products = GCSToBigQueryOperator(
        task_id = "gcs_to_bq_products",
        bucket='{}-bucket'.format(GCP_PROJECT_ID),
        external_table=True,
        source_objects=['products/products.csv'],
        destination_project_dataset_table='bronze.products',
        schema_fields = [
            {'name': 'productid', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'subcategory', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description_pt', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description_de', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description_es', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description_en', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description_zh', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'color', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'sizes', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'productioncost', 'type': 'FLOAT', 'mode': 'NULLABLE'},

        ],
        write_disposition='WRITE_TRUNCATE'
    )    




    gcs_to_bq_stores
    gcs_to_bq_employees
    gcs_to_bq_products

if __name__ == "__main__":
    dag.cli()
