-- Six SQL reporting views over the raw Kafka->NiFi->PostgreSQL records.
-- All six retain exactly the original raw event data in sales_raw.

CREATE OR REPLACE VIEW public.report_product_sales AS
WITH product_agg AS (
    SELECT product_name, product_category, product_brand,
           product_color, product_size, product_material,
           COUNT(*)::bigint AS sales_count,
           SUM(NULLIF(sale_quantity, '')::int)::bigint AS units_sold,
           SUM(NULLIF(sale_total_price, '')::numeric)::numeric(18,2) AS revenue,
           ROUND(AVG(NULLIF(product_rating, '')::numeric), 3) AS average_rating,
           MAX(NULLIF(product_reviews, '')::int) AS review_count
    FROM public.sales_raw
    GROUP BY product_name, product_category, product_brand,
             product_color, product_size, product_material
)
SELECT *, SUM(revenue) OVER (PARTITION BY product_category)::numeric(18,2) AS category_revenue,
       ROW_NUMBER() OVER (ORDER BY units_sold DESC, revenue DESC) AS sales_rank
FROM product_agg;

CREATE OR REPLACE VIEW public.report_customer_sales AS
WITH customer_agg AS (
    SELECT LOWER(TRIM(customer_email)) AS customer_email,
           MAX(customer_first_name) AS first_name,
           MAX(customer_last_name) AS last_name,
           MAX(customer_country) AS country,
           COUNT(*)::bigint AS sales_count,
           SUM(NULLIF(sale_total_price, '')::numeric)::numeric(18,2) AS total_spent,
           ROUND(AVG(NULLIF(sale_total_price, '')::numeric), 2) AS average_check
    FROM public.sales_raw
    GROUP BY LOWER(TRIM(customer_email))
)
SELECT *, COUNT(*) OVER (PARTITION BY country) AS customers_in_country,
       ROW_NUMBER() OVER (ORDER BY total_spent DESC, customer_email) AS spend_rank
FROM customer_agg;

CREATE OR REPLACE VIEW public.report_time_sales AS
WITH monthly AS (
    SELECT EXTRACT(YEAR FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::int AS year,
           EXTRACT(MONTH FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::int AS month,
           COUNT(*)::bigint AS sales_count,
           SUM(NULLIF(sale_quantity, '')::int)::bigint AS units_sold,
           SUM(NULLIF(sale_total_price, '')::numeric)::numeric(18,2) AS revenue,
           ROUND(AVG(NULLIF(sale_total_price, '')::numeric), 2) AS average_check
    FROM public.sales_raw
    GROUP BY 1, 2
)
SELECT *, LAG(revenue) OVER (ORDER BY year, month) AS previous_month_revenue,
       (revenue - LAG(revenue) OVER (ORDER BY year, month))::numeric(18,2) AS monthly_change,
       SUM(revenue) OVER (PARTITION BY year)::numeric(18,2) AS annual_revenue,
       SUM(sales_count) OVER (PARTITION BY year)::bigint AS annual_sales_count
FROM monthly;

CREATE OR REPLACE VIEW public.report_store_sales AS
WITH store_agg AS (
    SELECT LOWER(TRIM(store_email)) AS store_email,
           MAX(store_name) AS store_name,
           MAX(store_city) AS city,
           MAX(store_country) AS country,
           COUNT(*)::bigint AS sales_count,
           SUM(NULLIF(sale_total_price, '')::numeric)::numeric(18,2) AS revenue,
           ROUND(AVG(NULLIF(sale_total_price, '')::numeric), 2) AS average_check
    FROM public.sales_raw
    GROUP BY LOWER(TRIM(store_email))
)
SELECT *, SUM(sales_count) OVER (PARTITION BY city) AS city_sales_count,
       SUM(sales_count) OVER (PARTITION BY country) AS country_sales_count,
       ROW_NUMBER() OVER (ORDER BY revenue DESC, store_email) AS revenue_rank
FROM store_agg;

CREATE OR REPLACE VIEW public.report_supplier_sales AS
WITH supplier_agg AS (
    SELECT LOWER(TRIM(supplier_email)) AS supplier_email,
           MAX(supplier_name) AS supplier_name,
           MAX(supplier_country) AS country,
           COUNT(*)::bigint AS sales_count,
           SUM(NULLIF(sale_total_price, '')::numeric)::numeric(18,2) AS revenue,
           ROUND(AVG(NULLIF(product_price, '')::numeric), 2) AS average_product_price
    FROM public.sales_raw
    GROUP BY LOWER(TRIM(supplier_email))
)
SELECT *, SUM(sales_count) OVER (PARTITION BY country) AS country_sales_count,
       SUM(revenue) OVER (PARTITION BY country)::numeric(18,2) AS country_revenue,
       ROW_NUMBER() OVER (ORDER BY revenue DESC, supplier_email) AS revenue_rank
FROM supplier_agg;

CREATE OR REPLACE VIEW public.report_product_quality AS
WITH quality AS (
    SELECT product_name, product_category, product_brand, product_color,
           product_size, product_material,
           ROUND(AVG(NULLIF(product_rating, '')::numeric), 3) AS average_rating,
           MAX(NULLIF(product_reviews, '')::int) AS review_count,
           SUM(NULLIF(sale_quantity, '')::int)::bigint AS units_sold
    FROM public.sales_raw
    GROUP BY product_name, product_category, product_brand, product_color,
             product_size, product_material
), correlation AS (
    SELECT CORR(average_rating::double precision, units_sold::double precision)
           AS rating_sales_correlation
    FROM quality
)
SELECT q.*, c.rating_sales_correlation,
       ROW_NUMBER() OVER (ORDER BY average_rating DESC NULLS LAST, review_count DESC) AS rating_rank_desc,
       ROW_NUMBER() OVER (ORDER BY average_rating ASC NULLS LAST, review_count DESC) AS rating_rank_asc,
       ROW_NUMBER() OVER (ORDER BY review_count DESC NULLS LAST, average_rating DESC) AS review_rank
FROM quality q CROSS JOIN correlation c;
