-- Task 2b: Report query #1
-- periode, class, model, total
SELECT
    DATE_FORMAT(s.invoice_date, '%Y-%m') AS periode,
    CASE
        WHEN s.price_amount >= 100000000 AND s.price_amount < 250000000 THEN 'LOW'
        WHEN s.price_amount >= 250000000 AND s.price_amount <= 400000000 THEN 'MEDIUM'
        WHEN s.price_amount > 400000000 THEN 'HIGH'
        ELSE 'UNCLASSIFIED'
    END AS class,
    s.model,
    SUM(s.price_amount) AS total
FROM sales_clean s
GROUP BY
    DATE_FORMAT(s.invoice_date, '%Y-%m'),
    CASE
        WHEN s.price_amount >= 100000000 AND s.price_amount < 250000000 THEN 'LOW'
        WHEN s.price_amount >= 250000000 AND s.price_amount <= 400000000 THEN 'MEDIUM'
        WHEN s.price_amount > 400000000 THEN 'HIGH'
        ELSE 'UNCLASSIFIED'
    END,
    s.model
ORDER BY periode, class, s.model;


-- Task 2b: Report query #2
-- periode, vin, customer_name, address, count_service, priority
WITH service_count AS (
    SELECT
        DATE_FORMAT(a.service_date, '%Y') AS periode,
        a.vin,
        a.customer_id,
        COUNT(*) AS count_service
    FROM after_sales_clean a
    GROUP BY
        DATE_FORMAT(a.service_date, '%Y'),
        a.vin,
        a.customer_id
)
SELECT
    sc.periode,
    sc.vin,
    c.customer_name,
    COALESCE(al.address, '-') AS address,
    sc.count_service,
    CASE
        WHEN sc.count_service > 10 THEN 'HIGH'
        WHEN sc.count_service BETWEEN 5 AND 10 THEN 'MED'
        ELSE 'LOW'
    END AS priority
FROM service_count sc
LEFT JOIN customers_clean c
    ON c.id = sc.customer_id
LEFT JOIN customer_addresses_latest al
    ON al.customer_id = sc.customer_id
ORDER BY sc.periode, sc.count_service DESC, sc.vin;
