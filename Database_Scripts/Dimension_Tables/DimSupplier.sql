IF OBJECT_ID('DimSupplier', 'U') IS NOT NULL
    DROP TABLE DimSupplier;

CREATE TABLE DimSupplier (
    Supplier_Key INT IDENTITY(1,1) PRIMARY KEY,
    Supplier_ID INT NOT NULL UNIQUE,
    Supplier_Name NVARCHAR(255) NOT NULL,
    Supplier_Email NVARCHAR(255) NOT NULL,
    Source_System NVARCHAR(50) NOT NULL DEFAULT 'OLTP Ecommerce',
    
);
