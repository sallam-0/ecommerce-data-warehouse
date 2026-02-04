IF OBJECT_ID('DimCustomer', 'U') IS NOT NULL
    DROP TABLE DimCustomer;
GO

CREATE TABLE DimCustomer (
    
    Customer_Key INT IDENTITY(1,1) PRIMARY KEY,

    
    Customer_ID INT NOT NULL,
    FullName NVARCHAR(255), 
    Email NVARCHAR(255),
    Country NVARCHAR(255),
    Source_System NVARCHAR(50) DEFAULT 'OLTP Ecommerce',
    Start_date DATETIME NOT NULL DEFAULT GETDATE(),
    End_date DATETIME NULL, 
    Is_Current BIT NOT NULL DEFAULT 1 
);
