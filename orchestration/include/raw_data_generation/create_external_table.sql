-- Creating bronze.stores external table (CSV format)
CREATE OR REPLACE EXTERNAL TABLE `ecom-pipeline-gcp.bronze.stores`
OPTIONS (
    format = 'CSV',
    uris = ['gs://ecom-pipeline-gcp-bucket/stores/stores.csv'],
    skip_leading_rows = 1
);

-- Creating bronze.employees external table (CSV format)
CREATE OR REPLACE EXTERNAL TABLE `ecom-pipeline-gcp.bronze.employees`
OPTIONS (
    format = 'CSV',
    uris = ['gs://ecom-pipeline-gcp-bucket/employees/employees.csv'],
    skip_leading_rows = 1
);


-- Creating bronze.products external table (CSV format)
CREATE OR REPLACE EXTERNAL TABLE `ecom-pipeline-gcp.bronze.products`
OPTIONS (
    format = 'CSV',
    uris = ['gs://ecom-pipeline-gcp-bucket/products/products.csv'],
    skip_leading_rows = 1
);


-- Creating bronze.customers external table (CSV format)
CREATE OR REPLACE EXTERNAL TABLE `ecom-pipeline-gcp.bronze.customers`
OPTIONS (
    format = 'CSV',
    uris = ['gs://ecom-pipeline-gcp-bucket/customers/customers-*.csv'],
    skip_leading_rows = 1
);

-- TODO: Creating bronze.transactions external table (CSV format)

-- TODO: Creating bronze.discounts external table (CSV format)