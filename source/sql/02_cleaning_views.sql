-- Task 2a: Cleaning views for MySQL 8+

CREATE OR REPLACE VIEW customers_clean AS
SELECT
    c.id,
    TRIM(c.name) AS customer_name,
    CASE
        WHEN c.dob IS NULL THEN NULL
        WHEN STR_TO_DATE(c.dob, '%Y-%m-%d') IS NOT NULL THEN
            CASE
                WHEN STR_TO_DATE(c.dob, '%Y-%m-%d') = DATE('1900-01-01') THEN NULL
                ELSE STR_TO_DATE(c.dob, '%Y-%m-%d')
            END
        WHEN STR_TO_DATE(c.dob, '%Y/%m/%d') IS NOT NULL THEN STR_TO_DATE(c.dob, '%Y/%m/%d')
        WHEN STR_TO_DATE(c.dob, '%d/%m/%Y') IS NOT NULL THEN STR_TO_DATE(c.dob, '%d/%m/%Y')
        ELSE NULL
    END AS dob,
    CASE
        WHEN UPPER(TRIM(c.name)) LIKE 'PT %' THEN 'CORPORATE'
        ELSE 'INDIVIDUAL'
    END AS customer_type,
    c.created_at
FROM customers_raw c;

CREATE OR REPLACE VIEW sales_clean AS
SELECT
    vin,
    customer_id,
    model,
    invoice_date,
    CAST(REPLACE(price, '.', '') AS UNSIGNED) AS price_amount,
    created_at
FROM (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.vin, s.customer_id, s.model, s.invoice_date, s.price
            ORDER BY s.created_at ASC
        ) AS rn
    FROM sales_raw s
) t
WHERE t.rn = 1;

CREATE OR REPLACE VIEW after_sales_clean AS
SELECT
    service_ticket,
    vin,
    customer_id,
    model,
    service_date,
    UPPER(TRIM(service_type)) AS service_type,
    created_at
FROM (
    SELECT
        a.*,
        ROW_NUMBER() OVER (
            PARTITION BY a.service_ticket
            ORDER BY a.created_at ASC
        ) AS rn
    FROM after_sales_raw a
) x
WHERE x.rn = 1;

CREATE OR REPLACE VIEW customer_addresses_latest AS
SELECT
    z.customer_id,
    z.address,
    z.city,
    z.province,
    z.created_at
FROM (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_id
            ORDER BY c.created_at DESC, c.updated_at DESC
        ) AS rn
    FROM customer_addresses c
) z
WHERE z.rn = 1;
