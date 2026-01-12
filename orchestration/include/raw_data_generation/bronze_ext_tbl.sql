CREATE OR REPLACE EXTERNAL TABLE `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}`
OPTIONS (
  format = 'CSV',
  uris = ['{{ params.gcs_uri }}'],
  skip_leading_rows = {{ params.skip_leading_rows }}
);
