-- ============================================
-- SETUP
-- ============================================
CREATE WAREHOUSE IF NOT EXISTS ecommerce_wh;

CREATE OR REPLACE DATABASE ecommerce_db;

USE WAREHOUSE ecommerce_wh;
USE DATABASE ecommerce_db;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================
-- FILE FORMATS + STAGE
-- ============================================
USE SCHEMA bronze;

CREATE OR REPLACE FILE FORMAT csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('', 'NULL', 'null')
EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE FILE FORMAT json_format
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE STORAGE INTEGRATION s3_ecommerce_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::611052933811:role/ADMIN'
STORAGE_ALLOWED_LOCATIONS = ('s3://thequerycrew/s3floder2/');

CREATE OR REPLACE STAGE ecommerce_stage
URL = 's3://thequerycrew/s3floder2/'
STORAGE_INTEGRATION = s3_ecommerce_integration;

-- ============================================
-- BRONZE LAYER (RAW INGESTION)
-- ============================================

CREATE OR REPLACE TABLE raw_customers (
    customer_id STRING,
    customer_name STRING,
    city STRING,
    signup_date STRING,
    email STRING,
    phone_number STRING
);

CREATE OR REPLACE PIPE customers_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw_customers
FROM @ecommerce_stage/customers_500_messy_with_contact1.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';


CREATE OR REPLACE TABLE raw_orders (
    raw_data VARIANT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PIPE orders_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw_orders
FROM @ecommerce_stage/orders_500_messy.json
FILE_FORMAT = (FORMAT_NAME = 'json_format')
ON_ERROR = 'CONTINUE';


CREATE OR REPLACE TABLE raw_products (
    product_id STRING,
    product_name STRING,
    category STRING,
    price STRING
);

CREATE OR REPLACE PIPE products_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw_products
FROM @ecommerce_stage
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
PATTERN = '.*products.*\\.csv';

-- ============================================
-- SILVER LAYER (DATA CLEANING)
-- ============================================
USE SCHEMA silver;

CREATE OR REPLACE TABLE clean_customers AS
SELECT
    TRIM(customer_id) AS customer_id,
    INITCAP(TRIM(customer_name)) AS customer_name,
    INITCAP(TRIM(city)) AS city,
    TRY_TO_DATE(signup_date) AS signup_date,
    LOWER(TRIM(email)) AS email,
    TRIM(phone_number) AS phone_number
FROM bronze.raw_customers
WHERE customer_id IS NOT NULL
AND TRY_TO_NUMBER(customer_id) IS NOT NULL
AND email LIKE '%@%';

CREATE OR REPLACE TABLE clean_products AS
SELECT
    TRIM(product_id) AS product_id,
    INITCAP(TRIM(product_name)) AS product_name,
    INITCAP(TRIM(category)) AS category,
    TRY_TO_DOUBLE(price) AS price
FROM bronze.raw_products
WHERE TRY_TO_NUMBER(product_id) IS NOT NULL
AND TRY_TO_DOUBLE(price) > 0;

CREATE OR REPLACE TABLE clean_orders AS
SELECT
    TRY_TO_NUMBER(raw_data:order_id::STRING) AS order_id,
    TRY_TO_NUMBER(raw_data:customer_id::STRING) AS customer_id,
    TRY_TO_NUMBER(raw_data:product_id::STRING) AS product_id,
    TRY_TO_TIMESTAMP(raw_data:order_date::STRING) AS order_date,
    TRY_TO_NUMBER(raw_data:quantity::STRING) AS quantity,
    TRY_TO_DOUBLE(raw_data:price::STRING) AS price,
    UPPER(TRIM(raw_data:status::STRING)) AS status
FROM bronze.raw_orders
WHERE TRY_TO_NUMBER(raw_data:order_id::STRING) IS NOT NULL
AND TRY_TO_NUMBER(raw_data:customer_id::STRING) IS NOT NULL
AND TRY_TO_NUMBER(raw_data:product_id::STRING) IS NOT NULL
AND TRY_TO_NUMBER(raw_data:quantity::STRING) IS NOT NULL
AND TRY_TO_DOUBLE(raw_data:price::STRING) > 0;

-- ============================================
-- GOLD LAYER (STAR SCHEMA)
-- ============================================
USE SCHEMA gold;

CREATE OR REPLACE TABLE dim_customers AS
SELECT DISTINCT
    TRY_TO_NUMBER(customer_id) AS customer_id,
    customer_name,
    city,
    signup_date,
    email,
    phone_number
FROM silver.clean_customers;

CREATE OR REPLACE TABLE dim_products AS
SELECT DISTINCT
    TRY_TO_NUMBER(product_id) AS product_id,
    product_name,
    category,
    price
FROM silver.clean_products;

CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
    DATE(order_date) AS date_key,
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    DAY(order_date) AS day,
    MONTHNAME(order_date) AS month_name
FROM silver.clean_orders;

CREATE OR REPLACE TABLE fact_orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    DATE(order_date) AS date_key,
    quantity,
    price,
    quantity * price AS order_amount,
    status
FROM silver.clean_orders;

-- ============================================
-- STREAMS
-- ============================================
CREATE OR REPLACE STREAM orders_stream ON TABLE silver.clean_orders;

-- ============================================
-- TASK (INCREMENTAL LOAD USING MERGE)
-- ============================================
CREATE OR REPLACE TASK refresh_fact_orders
WAREHOUSE = ecommerce_wh
SCHEDULE = '60 MINUTE'
AS
MERGE INTO gold.fact_orders t
USING silver.clean_orders s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
    t.quantity = s.quantity,
    t.price = s.price,
    t.order_amount = s.quantity * s.price
WHEN NOT MATCHED THEN
INSERT VALUES (
    s.order_id,
    s.customer_id,
    s.product_id,
    DATE(s.order_date),
    s.quantity,
    s.price,
    s.quantity * s.price,
    s.status
);

ALTER TASK refresh_fact_orders RESUME;

-- ============================================
-- KPI LAYER (ANALYTICS)
-- ============================================

-- Total Revenue
SELECT SUM(order_amount) AS total_revenue FROM gold.fact_orders;

-- Customer Lifetime Value
SELECT
    c.customer_name,
    COUNT(f.order_id) AS total_orders,
    SUM(f.order_amount) AS lifetime_value
FROM gold.fact_orders f
JOIN gold.dim_customers c
ON f.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY lifetime_value DESC;

-- Top Products
SELECT
    p.product_name,
    SUM(f.order_amount) AS revenue
FROM gold.fact_orders f
JOIN gold.dim_products p
ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC;

-- Monthly Revenue
SELECT
    d.year,
    d.month,
    SUM(f.order_amount) AS revenue
FROM gold.fact_orders f
JOIN gold.dim_date d
ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- ============================================
-- SECURITY LAYER (RBAC)
-- ============================================

CREATE ROLE IF NOT EXISTS analyst_role;
CREATE ROLE IF NOT EXISTS engineer_role;

-- Analyst (read-only)
GRANT USAGE ON WAREHOUSE ecommerce_wh TO ROLE analyst_role;
GRANT USAGE ON DATABASE ecommerce_db TO ROLE analyst_role;
GRANT USAGE ON SCHEMA ecommerce_db.gold TO ROLE analyst_role;

GRANT SELECT ON ALL TABLES IN SCHEMA ecommerce_db.gold TO ROLE analyst_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ecommerce_db.gold TO ROLE analyst_role;

-- Engineer (full access)
GRANT USAGE ON WAREHOUSE ecommerce_wh TO ROLE engineer_role;
GRANT USAGE ON DATABASE ecommerce_db TO ROLE engineer_role;

GRANT USAGE ON SCHEMA ecommerce_db.bronze TO ROLE engineer_role;
GRANT USAGE ON SCHEMA ecommerce_db.silver TO ROLE engineer_role;
GRANT USAGE ON SCHEMA ecommerce_db.gold TO ROLE engineer_role;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ecommerce_db.bronze TO ROLE engineer_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ecommerce_db.silver TO ROLE engineer_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ecommerce_db.gold TO ROLE engineer_role;

GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ecommerce_db.bronze TO ROLE engineer_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ecommerce_db.silver TO ROLE engineer_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ecommerce_db.gold TO ROLE engineer_role;