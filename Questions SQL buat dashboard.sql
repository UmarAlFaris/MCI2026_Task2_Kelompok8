-- Ada Berapa Order

SELECT COUNT(DISTINCT order_id) AS total_orders 
FROM analytics.order_items;


-- Jumlah Item Per Keranjang

WITH basket_sizes AS (
    SELECT 
        order_id, 
        COUNT(product_id) AS basket_size
    FROM analytics.order_items
    GROUP BY order_id
)
SELECT 
    MIN(basket_size) AS min_basket_size,
    ROUND(AVG(basket_size), 1) AS mean_basket_size,
    MAX(basket_size) AS max_basket_size
FROM basket_sizes;


-- Ada Berapa Customer

SELECT COUNT(DISTINCT user_id) AS total_customers 
FROM analytics.order_items;


-- TOP 15 Produk Paling Sering Dibeli

SELECT 
    product_name, 
    COUNT(product_id) AS total_sold
FROM analytics.order_items
GROUP BY product_name
ORDER BY total_sold DESC
LIMIT 15;


-- Tipe-Tipe Produk yang Dibeli

SELECT 
    department, 
    COUNT(product_id) AS total_items_sold 
FROM analytics.order_items 
GROUP BY department 
ORDER BY total_items_sold DESC;


-- Graph Order per Jam

SELECT 
    order_hour_of_day, 
    COUNT(DISTINCT order_id) AS total_orders 
FROM analytics.order_items 
GROUP BY order_hour_of_day 
ORDER BY order_hour_of_day ASC;


-- Graph Order per Hari

SELECT 
    order_dow AS day_of_week, 
    COUNT(DISTINCT order_id) AS total_orders 
FROM analytics.order_items 
GROUP BY order_dow 
ORDER BY order_dow ASC;


-- Graph Hari Semenjak Last Order

SELECT 
    days_since_prior_order, 
    COUNT(DISTINCT order_id) AS total_orders 
FROM analytics.order_items 
WHERE days_since_prior_order IS NOT NULL
GROUP BY days_since_prior_order 
ORDER BY days_since_prior_order ASC
limit 15;


-- TOP 15 Produk Paling Reordered

SELECT 
    product_name, 
    SUM(reordered) AS total_reorders
FROM analytics.order_items
GROUP BY product_name
ORDER BY total_reorders DESC
LIMIT 15;