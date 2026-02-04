-- =============================================
-- Create FactReturns Fact Table
-- Grain: One row per return
-- =============================================

CREATE TABLE FactReturns (
    -- Surrogate Key
    FactReturns_Key BIGINT IDENTITY(1,1) NOT NULL,
    
    -- Foreign Keys to Dimensions
    ReturnDate_Key INT NOT NULL,                   -- FK to DimDate
    Customer_Key INT NOT NULL,                     -- FK to DimCustomer
    Product_Key INT NOT NULL,                      -- FK to DimProduct
    PaymentMethod_Key INT NOT NULL,                -- FK to DimPaymentMethod
    
    -- Degenerate Dimensions (Business Keys)
    Return_ID INT NOT NULL,                        -- Business key from returns table
    Order_ID INT NOT NULL,                         -- Business key linking to original order
    
    -- Measures (Facts)
    Amount_Refunded DECIMAL(18, 2) NOT NULL,
    
    -- Descriptive Attributes
    Reason NVARCHAR(255) NULL,                     -- Reason for return
    
    -- Audit Columns
    Created_Date DATETIME NOT NULL DEFAULT GETDATE(),
    Modified_Date DATETIME NOT NULL DEFAULT GETDATE(),
    
    -- Constraints
    CONSTRAINT PK_FactReturns PRIMARY KEY CLUSTERED (FactReturns_Key),
    CONSTRAINT FK_FactReturns_DimDate FOREIGN KEY (ReturnDate_Key) 
        REFERENCES DimDate(Date_Key),
    CONSTRAINT FK_FactReturns_DimCustomer FOREIGN KEY (Customer_Key) 
        REFERENCES DimCustomer(Customer_Key),
    CONSTRAINT FK_FactReturns_DimProduct FOREIGN KEY (Product_Key) 
        REFERENCES DimProduct(Product_Key),
    CONSTRAINT FK_FactReturns_DimPaymentMethod FOREIGN KEY (PaymentMethod_Key) 
        REFERENCES DimPaymentMethod(PaymentMethod_Key),
    
    -- Check Constraints
    CONSTRAINT CHK_FactReturns_AmountRefunded CHECK (Amount_Refunded >= 0),
    
    -- Unique Constraint on Return_ID (business key should be unique)
    CONSTRAINT UK_FactReturns_ReturnID UNIQUE (Return_ID)
);

