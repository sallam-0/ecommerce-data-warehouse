-- =============================================
-- STAGING TABLES FOR FACTS
-- =============================================

-- =============================================
-- STG_FactSales
-- =============================================
IF OBJECT_ID('STG_FactSales', 'U') IS NOT NULL
    DROP TABLE STG_FactSales;
GO

CREATE TABLE STG_FactSales (
    Order_ID INT,
    OrderItem_ID INT,
    Order_Date DATE,
    Customer_ID INT,
    Product_ID INT,
    Supplier_ID INT,
    PaymentMethod_ID INT,
    Campaign_ID INT,
    Quantity INT,
    Subtotal DECIMAL(10,2),
    Discount DECIMAL(10,2)
);


GO



-- =============================================
-- STG_Returns
-- =============================================
IF OBJECT_ID('STG_Returns', 'U') IS NOT NULL
    DROP TABLE STG_Returns;
GO

CREATE TABLE STG_Returns (
    Return_ID INT NOT NULL,
    Order_ID INT NULL,
    Product_ID INT NULL,
    Customer_ID INT NOT NULL,
    PaymentMethod_ID INT NOT NULL,
    Return_Date DATE NULL,
    Reason NVARCHAR(500) NULL,
    Amount_Refunded DECIMAL(18, 2) NULL,
    Load_Date DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_STG_Returns PRIMARY KEY CLUSTERED (Return_ID)
);
GO

CREATE INDEX IX_STG_Returns_ReturnDate ON STG_Returns(Return_Date);
CREATE INDEX IX_STG_Returns_OrderID ON STG_Returns(Order_ID);
GO





--Create Load Control Table For Incremental Load

CREATE TABLE ETL_LoadControl (
    TableName NVARCHAR(100) PRIMARY KEY,
    LastLoadDate DATETIME,
    LastUpdateDate DATETIME DEFAULT GETDATE()
);

GO
-- Initialize
INSERT INTO ETL_LoadControl (TableName, LastLoadDate)
VALUES ('FactSales', '1900-01-01');
