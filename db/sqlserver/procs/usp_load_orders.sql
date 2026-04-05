USE ERP_DEMO;
GO

CREATE OR ALTER PROC core.usp_load_orders
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @runId INT;

    -- Log ETL start
    INSERT INTO log.ETLRun DEFAULT VALUES;
    SET @runId = SCOPE_IDENTITY();

    BEGIN TRY
        -- Ensure all customers exist in core.Customer
        MERGE core.Customer AS tgt
        USING stg.Customer AS src
        ON tgt.CustomerCode = src.CustomerCode
        WHEN MATCHED THEN
            UPDATE SET tgt.CustomerName = src.CustomerName, tgt.UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (CustomerCode, CustomerName) VALUES (src.CustomerCode, src.CustomerName);

        -- Ensure all products exist in core.Product
        MERGE core.Product AS tgt
        USING stg.Product AS src
        ON tgt.SKU = src.SKU
        WHEN MATCHED THEN
            UPDATE SET tgt.ProductName = src.ProductName, tgt.UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (SKU, ProductName) VALUES (src.SKU, src.ProductName);

        -- Insert Orders
        INSERT INTO core.[Order] (ExternalOrderNo, OrderDate, BuyerID, SupplierID, Currency, Status)
        SELECT
            h.ExternalOrderNo,
            h.OrderDate,
            cb.CustomerID,
            cs.CustomerID,
            ISNULL(h.Currency,'EUR'),
            'RECEIVED'
        FROM stg.OrderHeader h
        JOIN core.Customer cb ON cb.CustomerCode = h.BuyerCode
        LEFT JOIN core.Customer cs ON cs.CustomerCode = h.SupplierCode
        WHERE NOT EXISTS (
            SELECT 1 FROM core.[Order] o WHERE o.ExternalOrderNo = h.ExternalOrderNo
        );

        -- Insert OrderLines
        INSERT INTO core.OrderLine (OrderID, [LineNo], ProductID, Quantity, UnitPrice)
        SELECT
            o.OrderID,
            l.[LineNo],
            p.ProductID,
            l.Quantity,
            l.UnitPrice
        FROM stg.OrderLine l
        JOIN core.[Order] o ON o.ExternalOrderNo = l.ExternalOrderNo
        JOIN core.Product p ON p.SKU = l.SKU
        WHERE NOT EXISTS (
            SELECT 1 FROM core.OrderLine ol
            WHERE ol.OrderID = o.OrderID AND ol.[LineNo] = l.[LineNo]
        );

        -- Log success
        UPDATE log.ETLRun
        SET Status = 'SUCCESS',
            FinishedAt = SYSUTCDATETIME(),
            Message = 'Load complete'
        WHERE RunID = @runId;

    END TRY
    BEGIN CATCH
        -- Log failure
        UPDATE log.ETLRun
        SET Status = 'FAILED',
            FinishedAt = SYSUTCDATETIME(),
            Message = ERROR_MESSAGE()
        WHERE RunID = @runId;
        THROW;
    END CATCH
END
GO
