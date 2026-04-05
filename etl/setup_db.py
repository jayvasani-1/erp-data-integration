import os, re
from pathlib import Path
from etl.db import connect

BASE = Path(__file__).resolve().parents[1]


def exec_sql(cur, sql: str):
    """Execute multiple SQL statements separated by GO"""
    statements = [s.strip() for s in re.split(r'^\s*GO\s*$', sql, flags=re.MULTILINE) if s.strip()]
    for stmt in statements:
        try:
            cur.execute(stmt)
        except Exception as e:
            print(f"⚠️ SQL execution error: {e}\nStatement:\n{stmt}\n")


def main():
    # 1️⃣ Connect to master and create database if missing
    cn_master = connect(db="master", autocommit=True)
    cur = cn_master.cursor()
    cur.execute("IF DB_ID('ERP_DEMO') IS NULL CREATE DATABASE ERP_DEMO;")
    cur.close()
    cn_master.close()
    print("✅ ERP_DEMO database exists or created.")

    # 2️⃣ Connect to ERP_DEMO
    cn = connect(db="ERP_DEMO", autocommit=True)
    cur = cn.cursor()

    # 3️⃣ Create schemas
    exec_sql(cur, """
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core');
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='stg') EXEC('CREATE SCHEMA stg');
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='log') EXEC('CREATE SCHEMA log');
    """)

    # 4️⃣ Create core tables
    exec_sql(cur, """
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
        BuyerID INT NOT NULL FOREIGN KEY REFERENCES core.Customer(CustomerID),
        SupplierID INT NULL FOREIGN KEY REFERENCES core.Customer(CustomerID),
        Currency NCHAR(3) NULL DEFAULT 'EUR',
        Status NVARCHAR(32) NOT NULL DEFAULT 'RECEIVED',
        CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

    IF OBJECT_ID('core.OrderLine','U') IS NULL
    CREATE TABLE core.OrderLine (
        OrderLineID BIGINT IDENTITY PRIMARY KEY,
        OrderID BIGINT NOT NULL FOREIGN KEY REFERENCES core.[Order](OrderID),
        [LineNo] INT NOT NULL,
        ProductID INT NOT NULL FOREIGN KEY REFERENCES core.Product(ProductID),
        Quantity DECIMAL(18,4) NOT NULL,
        UnitPrice DECIMAL(18,4) NULL,
        NetAmount AS (ROUND(Quantity * ISNULL(UnitPrice,0), 2)) PERSISTED,
        CONSTRAINT UX_OrderLine UNIQUE(OrderID, [LineNo])
    );
    """)

    # 5️⃣ Create staging tables
    exec_sql(cur, """
    IF OBJECT_ID('stg.Customer','U') IS NULL
    CREATE TABLE stg.Customer (
        CustomerCode NVARCHAR(50) NOT NULL,
        CustomerName NVARCHAR(200) NOT NULL,
        City NVARCHAR(100) NULL,
        Country NCHAR(2) NULL,
        UpdatedAt DATETIME2 NULL
    );

    IF OBJECT_ID('stg.Product','U') IS NULL
    CREATE TABLE stg.Product (
        SKU NVARCHAR(64) NOT NULL,
        ProductName NVARCHAR(200) NOT NULL,
        UoM NVARCHAR(16) NULL,
        ListPrice DECIMAL(18,4) NULL,
        UpdatedAt DATETIME2 NULL
    );

    IF OBJECT_ID('stg.OrderHeader','U') IS NULL
    CREATE TABLE stg.OrderHeader (
        ExternalOrderNo NVARCHAR(64) NOT NULL,
        OrderDate DATE NOT NULL,
        BuyerCode NVARCHAR(50) NOT NULL,
        SupplierCode NVARCHAR(50) NULL,
        Currency NCHAR(3) NULL
    );

    IF OBJECT_ID('stg.OrderLine','U') IS NULL
    CREATE TABLE stg.OrderLine (
        ExternalOrderNo NVARCHAR(64) NOT NULL,
        [LineNo] INT NOT NULL,
        SKU NVARCHAR(64) NOT NULL,
        Quantity DECIMAL(18,4) NOT NULL,
        UnitPrice DECIMAL(18,4) NULL
    );
    """)

    # 6️⃣ Create log table
    exec_sql(cur, """
    IF OBJECT_ID('log.ETLRun','U') IS NULL
    CREATE TABLE log.ETLRun (
        RunID INT IDENTITY PRIMARY KEY,
        StartedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        FinishedAt DATETIME2 NULL,
        Status NVARCHAR(20) NOT NULL DEFAULT 'STARTED',
        Message NVARCHAR(4000) NULL
    );
    """)

    # 7️⃣ Create stored procedures (safe quotes + N-string)
    exec_sql(cur, r"""
    IF OBJECT_ID('core.usp_stage_orders_from_flat','P') IS NULL
    EXEC(N'
    CREATE PROCEDURE core.usp_stage_orders_from_flat
    AS
    BEGIN
        INSERT INTO core.Customer(CustomerCode, CustomerName, City, Country)
        SELECT CustomerCode, CustomerName, City, Country FROM stg.Customer;

        INSERT INTO core.Product(SKU, ProductName, UoM, ListPrice)
        SELECT SKU, ProductName, UoM, ListPrice FROM stg.Product;
    END
    ');

    IF OBJECT_ID('core.usp_load_orders','P') IS NULL
    EXEC(N'
    CREATE PROCEDURE core.usp_load_orders
    AS
    BEGIN
        INSERT INTO core.[Order](ExternalOrderNo, OrderDate, BuyerID, SupplierID, Currency)
        SELECT oh.ExternalOrderNo, oh.OrderDate,
               c.CustomerID AS BuyerID,
               s.CustomerID AS SupplierID,
               oh.Currency
        FROM stg.OrderHeader oh
        LEFT JOIN core.Customer c ON c.CustomerCode = oh.BuyerCode
        LEFT JOIN core.Customer s ON s.CustomerCode = oh.SupplierCode;

        INSERT INTO core.OrderLine(OrderID, [LineNo], ProductID, Quantity, UnitPrice)
        SELECT o.OrderID, ol.[LineNo], p.ProductID, ol.Quantity, ol.UnitPrice
        FROM stg.OrderLine ol
        JOIN core.[Order] o ON o.ExternalOrderNo = ol.ExternalOrderNo
        JOIN core.Product p ON p.SKU = ol.SKU;
    END
    ');
    """)

    cur.close()
    cn.close()
    print("✅ Database, tables, and stored procedures created successfully.")


if __name__ == "__main__":
    main()
