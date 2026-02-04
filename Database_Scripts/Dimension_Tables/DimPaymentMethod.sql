IF OBJECT_ID('DimPaymentMethod', 'U') IS NOT NULL
    DROP TABLE DimPaymentMethod;

CREATE TABLE DimPaymentMethod (
    PaymentMethod_Key INT IDENTITY(1,1) PRIMARY KEY,
    PaymentMethod_ID INT NOT NULL UNIQUE,
    Payment_Method_Name NVARCHAR(255) NOT NULL,
    Source_System NVARCHAR(50) NOT NULL DEFAULT 'OLTP Ecommerce'
);

