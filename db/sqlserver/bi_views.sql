USE ERP_DEMO;
GO
IF OBJECT_ID('core.vDimCustomer','V') IS NOT NULL DROP VIEW core.vDimCustomer;
GO
CREATE VIEW core.vDimCustomer AS
SELECT CustomerID, CustomerCode, CustomerName, City, Country FROM core.Customer;
GO
IF OBJECT_ID('core.vDimProduct','V') IS NOT NULL DROP VIEW core.vDimProduct;
GO
CREATE VIEW core.vDimProduct AS
SELECT ProductID, SKU, ProductName, UoM, ListPrice FROM core.Product;
GO
IF OBJECT_ID('core.vFactSales','V') IS NOT NULL DROP VIEW core.vFactSales;
GO
CREATE VIEW core.vFactSales AS
SELECT o.OrderID, o.OrderDate, o.BuyerID, o.SupplierID,
       ol.OrderLineID, ol.LineNo, ol.ProductID, ol.Quantity, ol.UnitPrice, ol.NetAmount
FROM core.[Order] o
JOIN core.OrderLine ol ON ol.OrderID = o.OrderID;
GO
