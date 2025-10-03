--Step 1: Create the clean online_retail Table in the Intermediate Layer
CREATE TABLE IF NOT EXISTS `singular-node-473906-k7.intermediate.online_retail_clean` (
  invoice_no        STRING,
  stock_code        STRING,
  description       STRING,
  quantity          INT64,
  unit_price        NUMERIC,
  line_amount       NUMERIC,
  invoice_date      DATETIME,
  customer_id       INT64,
  country           STRING,
  row_key           STRING,
  ingestion_timestamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at        TIMESTAMP
)
PARTITION BY DATE(invoice_date)
CLUSTER BY stock_code, customer_id,ingestion_timestamp;

ALTER TABLE `singular-node-473906-k7.intermediate.online_retail_clean`
SET OPTIONS (require_partition_filter = TRUE);



--Step 2: Update Existing Active Records if There Are Changes and insert new records
MERGE `singular-node-473906-k7.intermediate.online_retail_clean` T
USING (
  SELECT
    TRIM(InvoiceNo)                             AS invoice_no,
    UPPER(TRIM(StockCode))                      AS stock_code,
    TRIM(Description)                           AS description,
    CAST(Quantity AS INT64)                     AS quantity,
    CAST(UnitPrice AS NUMERIC)                  AS unit_price,
    CAST(Quantity AS NUMERIC) * CAST(UnitPrice AS NUMERIC) AS line_amount,
    InvoiceDate                                  AS invoice_date,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(TRIM(CustomerID), r'\.0$', ''), '') AS INT64) AS customer_id,
    INITCAP(TRIM(Country))                      AS country,
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
      TRIM(InvoiceNo),
      UPPER(TRIM(StockCode)),
      InvoiceDate,
      SAFE_CAST(NULLIF(REGEXP_REPLACE(TRIM(CustomerID), r'\.0$', ''), '') AS INT64)
    )))) AS row_key
  FROM (
    -- collapse exact repeats in this Bronze batch
    SELECT t.*,
           ROW_NUMBER() OVER (
             PARTITION BY InvoiceNo, StockCode, InvoiceDate, COALESCE(CustomerID,'NOID'),
                          Quantity, UnitPrice
             ORDER BY InvoiceDate DESC
           ) rn
    FROM `singular-node-473906-k7.raw.online_retail_ext` t
  )
  WHERE rn = 1
    AND InvoiceNo  IS NOT NULL
    AND StockCode  IS NOT NULL
    AND Quantity   IS NOT NULL
    AND UnitPrice  IS NOT NULL
    AND Quantity <> 0
    AND UnitPrice > 0
    -- date filter can be updated based on frequqency of run daily/hourly
    AND DATE(InvoiceDate) BETWEEN DATE '2010-01-01' AND DATE '2011-12-31'
) S
ON T.row_key = S.row_key
-- date filter can be updated based on frequqency of run daily/hourly
AND DATE(T.invoice_date) BETWEEN DATE '2010-01-01' AND DATE '2011-12-31'
WHEN MATCHED AND (
  T.description IS DISTINCT FROM S.description OR
  T.quantity    IS DISTINCT FROM S.quantity    OR
  T.unit_price  IS DISTINCT FROM S.unit_price  OR
  T.line_amount IS DISTINCT FROM S.line_amount OR
  T.customer_id IS DISTINCT FROM S.customer_id OR
  T.country     IS DISTINCT FROM S.country
)
THEN UPDATE SET
  description = S.description,
  quantity    = S.quantity,
  unit_price  = S.unit_price,
  line_amount = S.line_amount,
  customer_id = S.customer_id,
  country     = S.country,
  updated_at  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (invoice_no, stock_code, description, quantity, unit_price, line_amount,
          invoice_date, customer_id, country, row_key,
          ingestion_timestamp, updated_at)
  VALUES (S.invoice_no, S.stock_code, S.description, S.quantity, S.unit_price, S.line_amount,
          S.invoice_date, S.customer_id, S.country, S.row_key,
          CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());