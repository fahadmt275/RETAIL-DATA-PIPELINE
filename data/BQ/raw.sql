CREATE EXTERNAL TABLE IF NOT EXISTS `singular-node-473906-k7.raw.online_retail_ext` (
  InvoiceNo   STRING,
  StockCode   STRING,
  Description STRING,
  Quantity    INT64,
  InvoiceDate DATETIME,
  UnitPrice   NUMERIC,
  CustomerID  STRING,
  Country     STRING,
  updated_at  STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://retail-data-project/landing/retailer-db/online_retail/*.json']
);