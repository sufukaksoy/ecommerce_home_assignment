CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE TABLE mart.fact_orders AS
SELECT
    CAST(order_id AS BIGINT) AS order_id,
    CAST(user_id AS BIGINT) AS user_id,
    LOWER(COALESCE(NULLIF(TRIM(status), ''), 'unknown')) AS order_status,
    CAST(created_at AS TIMESTAMP) AS created_at_utc,
    CAST(CAST(created_at AS TIMESTAMP) AS DATE) AS order_date,
    CAST(COALESCE(num_of_item, 0) AS BIGINT) AS num_of_item
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at DESC NULLS LAST) AS rn
    FROM staging.orders_raw
)
WHERE rn = 1;

CREATE OR REPLACE TABLE mart.fact_order_items AS
SELECT
    CAST(oi.id AS BIGINT) AS order_item_id,
    CAST(oi.order_id AS BIGINT) AS order_id,
    CAST(oi.user_id AS BIGINT) AS user_id,
    CAST(oi.product_id AS BIGINT) AS product_id,
    CAST(oi.inventory_item_id AS BIGINT) AS inventory_item_id,
    LOWER(COALESCE(NULLIF(TRIM(oi.status), ''), 'unknown')) AS item_status,
    CAST(oi.created_at AS TIMESTAMP) AS created_at_utc,
    CAST(COALESCE(oi.sale_price, 0) AS DOUBLE) AS sale_price,
    1::BIGINT AS quantity,
    CAST(COALESCE(oi.sale_price, 0) AS DOUBLE) AS line_revenue
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC NULLS LAST) AS rn
    FROM staging.order_items_raw
) oi
WHERE oi.rn = 1
  AND EXISTS (
      SELECT 1 FROM mart.fact_orders fo WHERE fo.order_id = CAST(oi.order_id AS BIGINT)
  );
