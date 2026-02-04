IF OBJECT_ID('DimCampaign', 'U') IS NOT NULL
    DROP TABLE DimCampaign;

CREATE TABLE DimCampaign (
    Campaign_Key INT IDENTITY(1,1) PRIMARY KEY,
    Campaign_ID INT NOT NULL UNIQUE,
    Campaign_Name NVARCHAR(255) NOT NULL,
    Offer_Week INT,
    Source_System NVARCHAR(50) NOT NULL DEFAULT 'OLTP Ecommerce'
);
