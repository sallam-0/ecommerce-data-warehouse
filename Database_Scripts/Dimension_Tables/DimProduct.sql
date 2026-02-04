IF OBJECT_ID('DimProduct', 'U') IS NOT NULL
    DROP TABLE DimProduct;

CREATE TABLE DimProduct (
    Product_Key INT IDENTITY(1,1) PRIMARY KEY,
    Product_ID INT NOT NULL,
    Product_Name NVARCHAR(255) NOT NULL,
    Description NVARCHAR(MAX),
    Price DECIMAL(10, 2),
    Category_Name NVARCHAR(100),
    Subcategory_Name NVARCHAR(100),
    Source_System NVARCHAR(50) NOT NULL DEFAULT 'OLTP Ecommerce',
    Start_Date DATETIME NOT NULL DEFAULT GETDATE(),
    End_Date DATETIME NULL,
    Is_Current BIT NOT NULL DEFAULT 1
);
