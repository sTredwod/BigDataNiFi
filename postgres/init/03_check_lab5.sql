-- Checks run after NiFi has consumed the Kafka topic.
SELECT 'raw_records' AS metric, COUNT(*)::text AS value FROM public.sales_raw
UNION ALL SELECT 'unique_events', COUNT(DISTINCT event_id)::text FROM public.sales_raw
UNION ALL SELECT 'source_files', COUNT(DISTINCT source_file)::text FROM public.sales_raw
UNION ALL SELECT 'total_revenue', SUM(NULLIF(sale_total_price,'')::numeric)::text FROM public.sales_raw;

SELECT source_file, COUNT(*) AS rows_count
FROM public.sales_raw GROUP BY source_file ORDER BY source_file;

SELECT 'products' AS report, COUNT(*) AS rows_count FROM public.report_product_sales
UNION ALL SELECT 'customers', COUNT(*) FROM public.report_customer_sales
UNION ALL SELECT 'time', COUNT(*) FROM public.report_time_sales
UNION ALL SELECT 'stores', COUNT(*) FROM public.report_store_sales
UNION ALL SELECT 'suppliers', COUNT(*) FROM public.report_supplier_sales
UNION ALL SELECT 'quality', COUNT(*) FROM public.report_product_quality;

SELECT year, month, sales_count, revenue, average_check
FROM public.report_time_sales ORDER BY year, month;

DO $$
DECLARE
    actual_count INTEGER;
    unique_count INTEGER;
    file_count INTEGER;
    invalid_count INTEGER;
BEGIN
    SELECT COUNT(*), COUNT(DISTINCT event_id), COUNT(DISTINCT source_file)
      INTO actual_count, unique_count, file_count FROM public.sales_raw;
    IF actual_count <> 10000 OR unique_count <> 10000 OR file_count <> 10 THEN
        RAISE EXCEPTION 'Data validation failed: rows=%, unique=%, files=%', actual_count, unique_count, file_count;
    END IF;
    SELECT COUNT(*) INTO invalid_count
      FROM (SELECT source_file FROM public.sales_raw GROUP BY source_file HAVING COUNT(*) <> 1000) s;
    IF invalid_count <> 0 THEN
        RAISE EXCEPTION 'Source-file row count validation failed';
    END IF;
    RAISE NOTICE 'SUCCESS: 10000 raw events, 10 CSV files, 1000 events per file';
END $$;
