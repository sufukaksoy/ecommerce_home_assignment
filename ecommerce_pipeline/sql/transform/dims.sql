CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE TABLE mart.dim_users AS
SELECT
    CAST(id AS BIGINT) AS user_id,
    COALESCE(NULLIF(TRIM(first_name), ''), 'Unknown') AS first_name,
    COALESCE(NULLIF(TRIM(last_name), ''), 'Unknown') AS last_name,
    LOWER(COALESCE(NULLIF(TRIM(email), ''), 'unknown@example.com')) AS email,
    CAST(age AS INTEGER) AS age,
    COALESCE(NULLIF(TRIM(gender), ''), 'U') AS gender,
    COALESCE(NULLIF(TRIM(city), ''), 'Unknown') AS city,
    COALESCE(NULLIF(TRIM(state), ''), 'Unknown') AS state,
    COALESCE(NULLIF(TRIM(country), ''), 'Unknown') AS country,
    COALESCE(NULLIF(TRIM(traffic_source), ''), 'Unknown') AS traffic_source,
    CAST(created_at AS TIMESTAMP) AS created_at_utc
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC NULLS LAST) AS rn
    FROM staging.users_raw
)
WHERE rn = 1;

CREATE OR REPLACE TABLE mart.dim_products AS
SELECT
    CAST(id AS BIGINT) AS product_id,
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown Product') AS product_name,
    COALESCE(NULLIF(TRIM(category), ''), 'Uncategorized') AS category,
    COALESCE(NULLIF(TRIM(brand), ''), 'Unknown') AS brand,
    COALESCE(NULLIF(TRIM(department), ''), 'Unknown') AS department,
    CAST(COALESCE(retail_price, 0) AS DOUBLE) AS retail_price,
    CAST(COALESCE(cost, 0) AS DOUBLE) AS cost,
    CAST(distribution_center_id AS BIGINT) AS distribution_center_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn
    FROM staging.products_raw
)
WHERE rn = 1;

CREATE OR REPLACE TABLE mart.dim_distribution_centers AS
SELECT
    CAST(id AS BIGINT) AS distribution_center_id,
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS center_name,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn
    FROM staging.distribution_centers_raw
)
WHERE rn = 1;
