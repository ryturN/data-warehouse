CREATE TABLE IF NOT EXISTS customers_raw (
  id BIGINT PRIMARY KEY,
  name VARCHAR(200),
  dob VARCHAR(20),
  created_at DATETIME(3)
);

CREATE TABLE IF NOT EXISTS sales_raw (
  vin VARCHAR(50),
  customer_id BIGINT,
  model VARCHAR(100),
  invoice_date DATE,
  price VARCHAR(30),
  created_at DATETIME(3)
);

CREATE TABLE IF NOT EXISTS after_sales_raw (
  service_ticket VARCHAR(100),
  vin VARCHAR(50),
  customer_id BIGINT,
  model VARCHAR(100),
  service_date DATE,
  service_type VARCHAR(20),
  created_at DATETIME(3)
);

-- Sample seed (idempotent)
INSERT IGNORE INTO customers_raw (id, name, dob, created_at) VALUES
(1, 'Antonio', '1998-08-04', '2025-03-01 14:24:40.012'),
(2, 'Brandon', '2001-04-21', '2025-03-02 08:12:54.003'),
(3, 'Charlie', '1980/11/15', '2025-03-02 11:20:02.391'),
(4, 'Dominikus', '14/01/1995', '2025-03-03 09:50:41.852'),
(5, 'Erik', '1900-01-01', '2025-03-03 17:22:03.198'),
(6, 'PT Black Bird', NULL, '2025-03-04 12:52:16.122');

INSERT INTO sales_raw (vin, customer_id, model, invoice_date, price, created_at) VALUES
('JIS8135SAD', 1, 'RAIZA', '2025-03-01', '350.000.000', '2025-03-01 14:24:40.012'),
('MAS8160POE', 3, 'RANGGO', '2025-05-19', '430.000.000', '2025-05-19 14:29:21.003'),
('JLK1368KDE', 4, 'INNAVO', '2025-05-22', '600.000.000', '2025-05-22 16:10:28.120'),
('JLK1869KDF', 6, 'VELOS', '2025-08-02', '390.000.000', '2025-08-02 14:04:31.021'),
('JLK1962KOP', 6, 'VELOS', '2025-08-02', '390.000.000', '2025-08-02 15:21:04.201');

INSERT INTO after_sales_raw (service_ticket, vin, customer_id, model, service_date, service_type, created_at) VALUES
('T124-kgu1', 'MAS8160POE', 3, 'RANGGO', '2025-07-11', 'BP', '2025-07-11 09:24:40.012'),
('T560-jga1', 'JLK1368KDE', 4, 'INNAVO', '2025-08-04', 'PM', '2025-08-04 10:12:54.003'),
('T521-oai8', 'POI1059IIK', 5, 'RAIZA', '2026-09-10', 'GR', '2026-09-10 12:45:02.391');
