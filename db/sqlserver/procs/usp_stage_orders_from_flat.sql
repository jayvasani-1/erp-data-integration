USE ERP_DEMO;
GO

CREATE OR ALTER PROC core.usp_stage_orders_from_flat
AS
BEGIN
    SET NOCOUNT ON;

    -- Optional: Count staged rows
    SELECT COUNT(*) AS RowsInHeader FROM stg.OrderHeader;
    SELECT COUNT(*) AS RowsInLine FROM stg.OrderLine;

    -- Insert into staging Customer table from headers if not exists
    INSERT INTO stg.Customer (CustomerCode, CustomerName)
    SELECT DISTINCT BuyerCode, 'Buyer ' + BuyerCode
    FROM stg.OrderHeader h
    WHERE NOT EXISTS (
        SELECT 1 FROM stg.Customer c WHERE c.CustomerCode = h.BuyerCode
    );

    INSERT INTO stg.Customer (CustomerCode, CustomerName)
    SELECT DISTINCT SupplierCode, 'Supplier ' + SupplierCode
    FROM stg.OrderHeader h
    WHERE SupplierCode IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM stg.Customer c WHERE c.CustomerCode = h.SupplierCode
      );

    -- Insert into staging Product table from lines if not exists
    INSERT INTO stg.Product (SKU, ProductName)
    SELECT DISTINCT SKU, 'Product ' + SKU
    FROM stg.OrderLine l
    WHERE NOT EXISTS (
        SELECT 1 FROM stg.Product p WHERE p.SKU = l.SKU
    );
END
GO
