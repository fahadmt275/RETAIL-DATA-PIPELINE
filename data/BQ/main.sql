--dim_date
CREATE TABLE IF NOT EXISTS `singular-node-473906-k7.main.dim_date` AS
WITH dates AS (
  SELECT d AS dt
  FROM UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2011-12-31')) AS d  -- adjust to your data range
)
SELECT
  FORMAT_DATE('%Y%m%d', dt) AS date_key,  -- PK
  dt AS date,
  EXTRACT(YEAR FROM dt) AS year,
  EXTRACT(QUARTER FROM dt) AS quarter,
  EXTRACT(MONTH FROM dt) AS month,
  EXTRACT(DAY FROM dt) AS day,
  EXTRACT(DAYOFWEEK FROM dt) AS day_of_week,
  IF(EXTRACT(DAYOFWEEK FROM dt) IN (1,7), TRUE, FALSE) AS is_weekend
  from dates
ORDER BY dt;


--dim_product
CREATE TABLE IF NOT EXISTS `singular-node-473906-k7.main.dim_product` (
  product_key STRING,
  stock_code  STRING,
  description STRING,
  updated_at  TIMESTAMP
)
CLUSTER BY stock_code;

MERGE `singular-node-473906-k7.main.dim_product` T
USING (
  SELECT
    TO_HEX(SHA256(stock_code)) AS product_key,
    stock_code AS stock_code,
    MAX_BY(description, invoice_date)       AS description   -- latest by business time
  FROM `singular-node-473906-k7.intermediate.online_retail_clean`
  WHERE DATE(invoice_date) BETWEEN DATE '2010-01-01' AND DATE '2011-12-31'  -- required: Silver is partitioned
  GROUP BY stock_code
) S
ON T.product_key = S.product_key
WHEN MATCHED THEN UPDATE SET
  T.stock_code  = S.stock_code,
  T.description = S.description,
  T.updated_at  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (product_key, stock_code, description, updated_at)
  VALUES (S.product_key, S.stock_code, S.description, CURRENT_TIMESTAMP());

--dim_customer

CREATE TABLE IF NOT EXISTS `singular-node-473906-k7.main.dim_customer` (
  customer_key STRING,   -- hash(customer_id)
  customer_id  INT64,
  country      STRING,
  updated_at   TIMESTAMP
);

MERGE `singular-node-473906-k7.main.dim_customer` T
USING (
  SELECT
    TO_HEX(SHA256(CAST(customer_id AS STRING))) AS customer_key,
    customer_id AS customer_id,
    MAX_BY(country, invoice_date) AS country
  FROM `singular-node-473906-k7.intermediate.online_retail_clean`
  WHERE customer_id IS NOT NULL
  AND DATE(invoice_date) BETWEEN DATE '2010-01-01' AND DATE '2011-12-31'
  GROUP BY customer_id
) S
ON T.customer_key = S.customer_key
WHEN MATCHED THEN
  UPDATE SET
    T.customer_id = S.customer_id,
    T.country     = S.country,
    T.updated_at  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (customer_key, customer_id, country,  updated_at)
  VALUES (S.customer_key, S.customer_id, S.country, CURRENT_TIMESTAMP());


--fct_sales

CREATE TABLE IF NOT EXISTS `singular-node-473906-k7.main.fct_sales` (
  invoice_no   STRING,
  product_key  STRING,     -- FK to dim_product
  customer_key STRING,     -- FK to dim_customer (nullable)
  date_key     STRING,     -- FK to dim_date (YYYYMMDD)
  invoice_date DATETIME,   -- business timestamp (from intermediate)
  quantity     INT64,
  unit_price   NUMERIC,
  line_amount  NUMERIC,
  is_return    BOOL ,       -- quantity < 0
  is_cancelled BOOL        -- invoice_no starts with 'C'/'c'
)
PARTITION BY DATE(invoice_date)
CLUSTER BY product_key, customer_key,invoice_no;


ALTER TABLE `singular-node-473906-k7.main.fct_sales`
SET OPTIONS (require_partition_filter = TRUE);


MERGE `singular-node-473906-k7.main.fct_sales` T
USING (
  SELECT
    s.invoice_no,
    TO_HEX(SHA256(UPPER(TRIM(s.stock_code)))) AS product_key,
    CASE WHEN s.customer_id IS NULL THEN NULL
         ELSE TO_HEX(SHA256(CAST(s.customer_id AS STRING)))
    END AS customer_key,
    FORMAT_DATE('%Y%m%d', DATE(s.invoice_date)) AS date_key,
    s.invoice_date,
    CAST(s.quantity AS INT64)  AS quantity,
    CAST(s.unit_price AS NUMERIC) AS unit_price,
    CAST(s.line_amount AS NUMERIC) AS line_amount,
    (s.quantity < 0) AS is_return,
    REGEXP_CONTAINS(s.invoice_no, r'^[Cc]')   AS is_cancelled
  FROM `singular-node-473906-k7.intermediate.online_retail_clean` s
  WHERE
    DATE(s.invoice_date) BETWEEN DATE '2010-01-01' AND DATE '2011-12-31'
    AND s.invoice_no IS NOT NULL
    AND s.stock_code IS NOT NULL
    AND s.quantity IS NOT NULL
    AND s.unit_price IS NOT NULL
) S
ON  T.invoice_no   = S.invoice_no
AND T.product_key  = S.product_key
AND IFNULL(T.customer_key, '') = IFNULL(S.customer_key, '')
AND T.invoice_date = S.invoice_date
AND DATE(T.invoice_date) BETWEEN DATE '2010-01-01' AND DATE '2011-12-31'

WHEN MATCHED THEN UPDATE SET
  T.quantity    = S.quantity,
  T.unit_price  = S.unit_price,
  T.line_amount = S.line_amount,
  T.is_return   = S.is_return,
  is_cancelled = S.is_cancelled,
  T.date_key    = S.date_key

WHEN NOT MATCHED THEN
  INSERT (invoice_no, product_key, customer_key, date_key, invoice_date,
          quantity, unit_price, line_amount, is_return, is_cancelled)
  VALUES (S.invoice_no, S.product_key, S.customer_key, S.date_key, S.invoice_date,
          S.quantity, S.unit_price, S.line_amount, S.is_return, S.is_cancelled);
