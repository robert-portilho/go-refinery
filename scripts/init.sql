IF NOT EXISTS (
    SELECT *
    FROM sys.databases
    WHERE
        name = 'mydb'
) BEGIN EXEC ('CREATE DATABASE mydb');

END

USE mydb;
GO

IF NOT EXISTS (
    SELECT *
    FROM sys.tables
    WHERE
        name = 'ProcessedOrders'
) BEGIN
CREATE TABLE ProcessedOrders (
    id INT IDENTITY(1, 1) PRIMARY KEY,
    customer_id VARCHAR(50),
    total_value DECIMAL(10, 2),
    email_masked VARCHAR(100),
    processed_at DATETIME DEFAULT GETDATE()
);

END