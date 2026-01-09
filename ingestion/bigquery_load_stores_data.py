from google.cloud import bigquery

PROJECT_ID = "<your-project_id>"
TABLE_ID = "{}.bronze.stores".format(PROJECT_ID)
GCS_URI = "gs://{}-bucket/stores/stores.csv".format(PROJECT_ID)

def load_gcs_to_bigquery_snapshot_data(GCS_URI, TABLE_ID, table_schema):
    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        schema=table_schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE'
    )

    load_job = client.load_table_from_uri(GCS_URI, TABLE_ID, job_config=job_config)

    load_job.result()
    table = client.get_table(TABLE_ID)

    print("Loaded {} rows to table {}".format(table.num_rows, TABLE_ID))


bigquery_table_schema = [
    bigquery.SchemaField("store_id", "INTEGER"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("store_name", "STRING"),
    bigquery.SchemaField("number_of_employees", "STRING"),
    bigquery.SchemaField("zip_code", "STRING"),
    bigquery.SchemaField("latitude", "FLOAT64"),
    bigquery.SchemaField("longitude", "FLOAT64"),
]    


if __name__ == '__main__':
    load_gcs_to_bigquery_snapshot_data(
        GCS_URI, TABLE_ID, bigquery_table_schema)