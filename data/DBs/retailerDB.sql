-- using provided sample data set a table was created in Cloud SQL on GCP and sample dataset was loaded into this table using cloud shell
-- , following which data ingestion was simulated assuming online_retail to be source DB

CREATE TABLE `online_retail` (
    `InvoiceNo` varchar(20) DEFAULT NULL,
    `StockCode` varchar(20) DEFAULT NULL,
    `Description` varchar(255) DEFAULT NULL,
    `Quantity` int DEFAULT NULL,
    `InvoiceDate` datetime DEFAULT NULL,
    `UnitPrice` decimal(10,2) DEFAULT NULL,
    `CustomerID` varchar(20) DEFAULT NULL,
    `Country` varchar(50) DEFAULT NULL,
    `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP )