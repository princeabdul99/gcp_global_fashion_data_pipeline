import json
import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

from airflow.sdk import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator 
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PATH_TO_SQL_SCRIPT = Path("/opt/airflow/include/raw_data_generation/create_external_table.sql")



load_dotenv()


args = {'owner': 'abdul-dev'}



with open(PATH_TO_SQL_SCRIPT, "r") as file:
    CREATE_EXTERNAL_TABLE_SQL = file.read()




def read_json_schema(gcs_file_path):
    with open(gcs_file_path, "r") as file:
        schema_json = json.load(file)
    
    return schema_json    


# Environment Variables
GCP_PROJECT_ID = os.getenv('PROJECT_ID')

# Airflow Variables
# settings = Variable.get("dag_settings", default_var="gcp")

# DAG Variables
# gcs_source_data_bucket = settings['gcs_source_data_bucket']
# bq_bronze_dataset = settings['bq_bronze_dataset']


with DAG(
    dag_id = 'dag_data_load_bigquery',
    default_args=args,
    schedule='0 5 * * *',
    start_date=datetime(2026, 1, 1),

) as dag:
    bq_create_external_tbl = BigQueryInsertJobOperator(
        task_id="bq_create_external_tbl",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": CREATE_EXTERNAL_TABLE_SQL,
                "useLegacySql": False
            }
        },
    )





    bq_create_external_tbl

if __name__ == "__main__":
    dag.cli()
