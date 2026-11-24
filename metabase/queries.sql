-- In Metabase, choose New -> SQL query, then select the PostgreSQL database.
-- Save each result as a question and add it to dashboard "BigDataNiFi — Sales Analytics".

-- 1. Product units sold (horizontal bar)
SELECT product_name, product_category, SUM(units_sold) AS units_sold
FROM public.report_product_sales
GROUP BY product_name, product_category
ORDER BY units_sold DESC LIMIT 10;

-- 2. Customers total spend (bar)
SELECT customer_email, country, total_spent
FROM public.report_customer_sales ORDER BY total_spent DESC LIMIT 10;

-- 3. Revenue by month (line)
SELECT make_date(year, month, 1) AS month_start, revenue, sales_count, average_check
FROM public.report_time_sales ORDER BY month_start;

-- 4. Top 5 stores (bar)
SELECT store_name, city, country, revenue
FROM public.report_store_sales ORDER BY revenue DESC LIMIT 5;

-- 5. Top 5 suppliers (bar)
SELECT supplier_name, country, revenue, average_product_price
FROM public.report_supplier_sales ORDER BY revenue DESC LIMIT 5;

-- 6. Product quality / rating (scatter or bar)
SELECT product_name, product_category, average_rating, units_sold, review_count,
       rating_sales_correlation
FROM public.report_product_quality
ORDER BY average_rating DESC, review_count DESC LIMIT 20;
