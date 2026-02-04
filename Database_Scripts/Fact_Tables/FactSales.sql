-- =============================================
-- Create FactSales Fact Table
-- Grain: One row per order item
-- =============================================
DROP TABLE FactSales;
CREATE TABLE FactSales (
    -- Surrogate Key
    FactSales_Key BIGINT IDENTITY(1,1) NOT NULL,
    
    -- Foreign Keys to Dimensions
    OrderDate_Key INT NOT NULL,                    -- FK to DimDate
    Customer_Key INT NOT NULL,                     -- FK to DimCustomer
    Product_Key INT NOT NULL,                      -- FK to DimProduct
    Supplier_Key INT NOT NULL,                     -- FK to DimSupplier
    PaymentMethod_Key INT NOT NULL,                -- FK to DimPaymentMethod
    Campaign_Key INT NULL,

    -- Degenerate Dimensions (Business Keys)
    Order_ID INT NOT NULL,                         -- Business key from orders table
    OrderItem_ID INT NOT NULL,                     -- Business key from orderitem table
    
    -- Measures (Facts)
    Quantity INT NOT NULL,
    Subtotal DECIMAL(18, 2) NOT NULL,
    Discount DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
    LineTotal AS (Subtotal - (Discount)) PERSISTED,  -- Calculated column
    
    -- Audit Columns
    Created_Date DATETIME NOT NULL DEFAULT GETDATE(),
    Modified_Date DATETIME NOT NULL DEFAULT GETDATE(),
    
    -- Constraints
    CONSTRAINT PK_FactSales PRIMARY KEY CLUSTERED (FactSales_Key),
    CONSTRAINT FK_FactSales_DimDate FOREIGN KEY (OrderDate_Key) 
        REFERENCES DimDate(Date_Key),
    CONSTRAINT FK_FactSales_DimCustomer FOREIGN KEY (Customer_Key) 
        REFERENCES DimCustomer(Customer_Key),
    CONSTRAINT FK_FactSales_DimProduct FOREIGN KEY (Product_Key) 
        REFERENCES DimProduct(Product_Key),
    CONSTRAINT FK_FactSales_DimSupplier FOREIGN KEY (Supplier_Key) 
        REFERENCES DimSupplier(Supplier_Key),
    CONSTRAINT FK_FactSales_DimPaymentMethod FOREIGN KEY (PaymentMethod_Key) 
        REFERENCES DimPaymentMethod(PaymentMethod_Key),
    CONSTRAINT FK_FactSales_DimCampaign FOREIGN KEY (Campaign_Key) 
        REFERENCES DimCampaign(Campaign_Key),
 
    
    -- Check Constraints
    CONSTRAINT CHK_FactSales_Quantity CHECK (Quantity > 0),
    CONSTRAINT CHK_FactSales_Subtotal CHECK (Subtotal >= 0),
    CONSTRAINT CHK_FactSales_Discount CHECK (Discount >= 0),
    CONSTRAINT CHK_FactSales_Discount_NotGreaterThanSubtotal CHECK (Discount <= Subtotal)
);
