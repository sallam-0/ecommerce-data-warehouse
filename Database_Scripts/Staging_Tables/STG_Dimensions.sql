-- =============================================
-- STAGING TABLES FOR DIMENSIONS
-- =============================================

-- =============================================
-- STG_Customer
-- =============================================
IF OBJECT_ID('STG_Customer', 'U') IS NOT NULL
    DROP TABLE STG_Customer;
GO

CREATE TABLE STG_Customer (
    Customer_ID INT NOT NULL,
    FullName NVARCHAR(255) NULL,
    Email NVARCHAR(255) NULL,
    Country NVARCHAR(255) NULL,
    Load_Date DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_STG_Customer PRIMARY KEY CLUSTERED (Customer_ID)
);
GO

-- =============================================
-- STG_Product
-- =============================================
IF OBJECT_ID('STG_Product', 'U') IS NOT NULL
    DROP TABLE STG_Product;
GO

CREATE TABLE STG_Product (
    Product_ID INT NOT NULL,
    Product_Name NVARCHAR(255) NULL,
    Description NVARCHAR(MAX) NULL,
    Price DECIMAL(10, 2) NULL,
    Category_Name NVARCHAR(100) NULL,
    Subcategory_Name NVARCHAR(100) NULL,
    Load_Date DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_STG_Product PRIMARY KEY CLUSTERED (Product_ID)
);
GO

-- =============================================
-- STG_Supplier
-- =============================================
IF OBJECT_ID('STG_Supplier', 'U') IS NOT NULL
    DROP TABLE STG_Supplier;
GO

CREATE TABLE STG_Supplier (
    Supplier_ID INT NOT NULL,
    Supplier_Name NVARCHAR(255) NULL,
    Supplier_Email NVARCHAR(255),
    Load_Date DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_STG_Supplier PRIMARY KEY CLUSTERED (Supplier_ID)
);
GO

-- =============================================
-- STG_PaymentMethod
-- =============================================
IF OBJECT_ID('STG_PaymentMethod', 'U') IS NOT NULL
    DROP TABLE STG_PaymentMethod;
GO

CREATE TABLE STG_PaymentMethod (
    PaymentMethod_ID INT NOT NULL,
    Payment_Method_Name NVARCHAR(255) NULL,
    Load_Date DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_STG_PaymentMethod PRIMARY KEY CLUSTERED (PaymentMethod_ID)
);
GO

-- =============================================
-- STG_Campaign
-- =============================================
IF OBJECT_ID('STG_Campaign', 'U') IS NOT NULL
    DROP TABLE STG_Campaign;
GO

CREATE TABLE STG_Campaign (
    Campaign_ID INT NOT NULL,
    Campaign_Name NVARCHAR(255) NULL,
    Offer_Week INT,
    Load_Date DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_STG_Campaign PRIMARY KEY CLUSTERED (Campaign_ID)
);
GO
