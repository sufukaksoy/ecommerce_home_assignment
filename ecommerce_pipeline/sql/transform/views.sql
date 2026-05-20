CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW mart.daily_commerce_metrics AS
WITH daily_totals AS (
    SELECT
        fo.order_date,
        COUNT(DISTINCT fo.order_id) AS daily_order_volume,
        SUM(foi.line_revenue) AS daily_revenue
    FROM mart.fact_orders fo
    JOIN mart.fact_order_items foi
      ON fo.order_id = foi.order_id
    WHERE fo.order_status NOT IN ('cancelled', 'canceled')
      AND LOWER(foi.item_status) NOT IN ('cancelled', 'canceled', 'returned')
    GROUP BY 1
),
category_totals AS (
    SELECT
        fo.order_date,
        dp.category,
        COUNT(DISTINCT fo.order_id) AS category_order_volume,
        COUNT(*) AS units_sold,
        SUM(foi.line_revenue) AS category_revenue
    FROM mart.fact_orders fo
    JOIN mart.fact_order_items foi
      ON fo.order_id = foi.order_id
    LEFT JOIN mart.dim_products dp
      ON foi.product_id = dp.product_id
    WHERE fo.order_status NOT IN ('cancelled', 'canceled')
      AND LOWER(foi.item_status) NOT IN ('cancelled', 'canceled', 'returned')
    GROUP BY 1, 2
)
SELECT
    ct.order_date,
    ct.category,
    ct.category_order_volume,
    ct.units_sold,
    ct.category_revenue,
    dt.daily_order_volume,
    dt.daily_revenue
FROM category_totals ct
JOIN daily_totals dt
  ON ct.order_date = dt.order_date
ORDER BY ct.order_date, ct.category_revenue DESC;

CREATE OR REPLACE VIEW mart.daily_summary_metrics AS
SELECT
    order_date,
    daily_order_volume,
    daily_revenue
FROM mart.daily_commerce_metrics
GROUP BY order_date, daily_order_volume, daily_revenue
ORDER BY order_date;

CREATE OR REPLACE VIEW mart.daily_top_category AS
SELECT order_date, category, category_revenue
FROM (
    SELECT
        order_date,
        category,
        category_revenue,
        ROW_NUMBER() OVER (PARTITION BY order_date ORDER BY category_revenue DESC) AS rn
    FROM mart.daily_commerce_metrics
)
WHERE rn = 1;
