%sql
-- =========================================
-- SQL ANALYTICS QUERIES
-- =========================================

-- 1. Top 10 products by revenue
SELECT
    product_id,
    product_name,
    category,
    total_units_sold,
    total_revenue
FROM gold_product_sales_summary
ORDER BY total_revenue DESC
LIMIT 10;

-- 2. Top customers by revenue
SELECT
    customer_id,
    full_name,
    country,
    total_orders,
    total_revenue
FROM gold_customer_summary
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Monthly revenue trend
SELECT
    order_month,
    ROUND(SUM(order_total), 2) AS total_revenue,
    ROUND(SUM(CASE WHEN is_paid = true THEN order_total ELSE 0 END), 2) AS paid_revenue,
    COUNT(DISTINCT order_id) AS total_orders
FROM gold_order_facts
GROUP BY order_month
ORDER BY order_month;

-- 4. Payment success rate by month
SELECT
    order_month,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(CASE WHEN is_paid = true THEN 1 ELSE 0 END) AS paid_orders,
    ROUND(
        100.0 * SUM(CASE WHEN is_paid = true THEN 1 ELSE 0 END) / COUNT(DISTINCT order_id),
        2
    ) AS payment_success_rate_pct
FROM gold_order_facts
GROUP BY order_month
ORDER BY order_month;

-- 5. Cancelled orders by country
SELECT
    country,
    COUNT(DISTINCT order_id) AS cancelled_orders
FROM gold_order_facts
WHERE order_status = 'cancelled'
GROUP BY country
ORDER BY cancelled_orders DESC;

-- 6. Orders with payment mismatch
SELECT
    order_id,
    customer_id,
    full_name,
    product_name,
    order_total,
    amount_paid,
    payment_gap,
    payment_status
FROM gold_order_facts
WHERE payment_gap <> 0
ORDER BY ABS(payment_gap) DESC;