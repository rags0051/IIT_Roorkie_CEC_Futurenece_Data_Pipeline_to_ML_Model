CREATE OR REPLACE TABLE raw_orders AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS VARCHAR) AS _loaded_at,
       'orders.csv' AS _source_file,
       :run_id AS _run_id
FROM read_csv_auto('/usr/local/airflow/include/data/nammamart/orders.csv', all_varchar=true);

CREATE OR REPLACE TABLE raw_payments AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS VARCHAR) AS _loaded_at,
       'payments.csv' AS _source_file,
       :run_id AS _run_id
FROM read_csv_auto('/usr/local/airflow/include/data/nammamart/payments.csv', all_varchar=true);

CREATE OR REPLACE TABLE raw_customers AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS VARCHAR) AS _loaded_at,
       'customers.csv' AS _source_file,
       :run_id AS _run_id
FROM read_csv_auto('/usr/local/airflow/include/data/nammamart/customers.csv', all_varchar=true);

CREATE OR REPLACE TABLE raw_products AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS VARCHAR) AS _loaded_at,
       'products.csv' AS _source_file,
       :run_id AS _run_id
FROM read_csv_auto('/usr/local/airflow/include/data/nammamart/products.csv', all_varchar=true);

CREATE OR REPLACE TABLE raw_stores AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS VARCHAR) AS _loaded_at,
       'stores.csv' AS _source_file,
       :run_id AS _run_id
FROM read_csv_auto('/usr/local/airflow/include/data/nammamart/stores.csv', all_varchar=true);
