USE ERP_DEMO;
GO

CREATE OR ALTER PROC core.usp_upsert_product
  @SKU NVARCHAR(64),
  @ProductName NVARCHAR(200),
  @UoM NVARCHAR(16) = 'EA',
  @ListPrice DECIMAL(18,4) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @runId INT;
    INSERT INTO log.ETLRun DEFAULT VALUES;
    SET @runId = SCOPE_IDENTITY();

    BEGIN TRY
        MERGE core.Product AS t
        USING (
            SELECT 
                @SKU AS SKU, 
                @ProductName AS ProductName, 
                @UoM AS UoM, 
                @ListPrice AS ListPrice
        ) AS s
        ON t.SKU = s.SKU
        WHEN MATCHED THEN 
            UPDATE SET 
                t.ProductName = s.ProductName,
                t.UoM = ISNULL(s.UoM, t.UoM),
                t.ListPrice = s.ListPrice,
                t.UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN 
            INSERT (SKU, ProductName, UoM, ListPrice, CreatedAt)
            VALUES (s.SKU, s.ProductName, ISNULL(s.UoM, 'EA'), s.ListPrice, SYSUTCDATETIME());

        -- Log success
        UPDATE log.ETLRun
        SET Status = 'SUCCESS',
            FinishedAt = SYSUTCDATETIME(),
            Message = 'Product upserted'
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
