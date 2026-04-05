USE ERP_DEMO;
GO

CREATE OR ALTER PROC core.usp_upsert_customer
  @CustomerCode NVARCHAR(50),
  @CustomerName NVARCHAR(200),
  @City NVARCHAR(100) = NULL,
  @Country NCHAR(2) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @runId INT;
    INSERT INTO log.ETLRun DEFAULT VALUES;
    SET @runId = SCOPE_IDENTITY();

    BEGIN TRY
        MERGE core.Customer AS t
        USING (
            SELECT 
                @CustomerCode AS CustomerCode, 
                @CustomerName AS CustomerName, 
                @City AS City, 
                @Country AS Country
        ) AS s
        ON t.CustomerCode = s.CustomerCode
        WHEN MATCHED THEN 
            UPDATE SET 
                t.CustomerName = s.CustomerName, 
                t.City = s.City, 
                t.Country = s.Country, 
                t.UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN 
            INSERT (CustomerCode, CustomerName, City, Country, CreatedAt) 
            VALUES (s.CustomerCode, s.CustomerName, s.City, s.Country, SYSUTCDATETIME());

        -- Log success
        UPDATE log.ETLRun
        SET Status = 'SUCCESS',
            FinishedAt = SYSUTCDATETIME(),
            Message = 'Customer upserted'
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
