-- Create database if it does not exist
IF DB_ID('ERP_DEMO') IS NULL 
BEGIN 
    CREATE DATABASE ERP_DEMO; 
END
GO

USE ERP_DEMO;
GO

-- Create schemas if missing
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='core') 
    EXEC('CREATE SCHEMA core');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='stg') 
    EXEC('CREATE SCHEMA stg');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='log') 
    EXEC('CREATE SCHEMA log');
GO

-- Core tables
IF OBJECT_ID('core.Customer','U') IS NULL
CREATE TABLE core.Customer (
    CustomerID INT IDENTITY PRIMARY KEY,
    CustomerCode NVARCHAR(50) UNIQUE NOT NULL,
    CustomerName NVARCHAR(200) NOT NULL,
    City NVARCHAR(100) NULL,
    Country NCHAR(2) NULL,
    CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL
);

IF OBJECT_ID('core.Product','U') IS NULL
CREATE TABLE core.Product (
    ProductID INT IDENTITY PRIMARY KEY,
    SKU NVARCHAR(64) UNIQUE NOT NULL,
    ProductName NVARCHAR(200) NOT NULL,
    UoM NVARCHAR(16) NOT NULL DEFAULT 'EA',
    ListPrice DECIMAL(18,4) NULL,
    CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL
);

IF OBJECT_ID('core.[Order]','U') IS NULL
CREATE TABLE core.[Order] (
    OrderID BIGINT IDENTITY PRIMARY KEY,
    ExternalOrderNo NVARCHAR(64) UNIQUE NOT NULL,
    OrderDate DATE NOT NULL,
    BuyerID INT NOT NULL,
    SupplierID INT NULL,
    Currency NVARCHAR(3) NULL DEFAULT 'EUR',
    Status NVARCHAR(32) NOT NULL DEFAULT 'RECEIVED',
    CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_Order_Buyer FOREIGN KEY (BuyerID) REFERENCES core.Customer(CustomerID),
    CONSTRAINT FK_Order_Supplier FOREIGN KEY (SupplierID) REFERENCES core.Customer(CustomerID)
);

IF OBJECT_ID('core.OrderLine','U') IS NULL
CREATE TABLE core.OrderLine (
    OrderLineID BIGINT IDENTITY PRIMARY KEY,
    OrderID BIGINT NOT NULL,
    [LineNo] INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity DECIMAL(18,4) NOT NULL,
    UnitPrice DECIMAL(18,4) NULL,
    NetAmount AS (ROUND(Quantity * ISNULL(UnitPrice,0), 2)) PERSISTED,
    CONSTRAINT UX_OrderLine UNIQUE(OrderID, [LineNo]),
    CONSTRAINT FK_OrderLine_Order FOREIGN KEY (OrderID) REFERENCES core.[Order](OrderID),
    CONSTRAINT FK_OrderLine_Product FOREIGN KEY (ProductID) REFERENCES core.Product(ProductID)
);

-- Staging tables
IF OBJECT_ID('stg.Customer','U') IS NULL
CREATE TABLE stg.Customer (
    CustomerCode NVARCHAR(50) NOT NULL,
    CustomerName NVARCHAR(200) NOT NULL,
    City NVARCHAR(100) NULL,
    Country NCHAR(2) NULL
);

IF OBJECT_ID('stg.Product','U') IS NULL
CREATE TABLE stg.Product (
    SKU NVARCHAR(64) NOT NULL,
    ProductName NVARCHAR(200) NOT NULL,
    UoM NVARCHAR(16) NULL,
    ListPrice DECIMAL(18,4) NULL
);

IF OBJECT_ID('stg.OrderHeader','U') IS NULL
CREATE TABLE stg.OrderHeader (
    ExternalOrderNo NVARCHAR(64) NOT NULL,
    OrderDate DATE NOT NULL,
    BuyerCode NVARCHAR(50) NOT NULL,
    SupplierCode NVARCHAR(50) NULL,
    Currency NVARCHAR(3) NULL
);

IF OBJECT_ID('stg.OrderLine','U') IS NULL
CREATE TABLE stg.OrderLine (
    ExternalOrderNo NVARCHAR(64) NOT NULL,
    [LineNo] INT NOT NULL,
    SKU NVARCHAR(64) NOT NULL,
    Quantity DECIMAL(18,4) NOT NULL,
    UnitPrice DECIMAL(18,4) NULL
);

-- Logging table
IF OBJECT_ID('log.ETLRun','U') IS NULL
CREATE TABLE log.ETLRun (
    RunID INT IDENTITY PRIMARY KEY,
    StartedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    FinishedAt DATETIME2 NULL,
    Status NVARCHAR(20) NOT NULL DEFAULT 'STARTED',
    Message NVARCHAR(4000) NULL
);
