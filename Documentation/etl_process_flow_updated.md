# ETL Process Flow - Retail Analytics Data Warehouse

## Overview

This document outlines the complete ETL (Extract, Transform, Load) process for the Retail Analytics Data Warehouse. The ETL process extracts data from the OLTP source system, transforms it according to business rules, and loads it into the dimensional model for analytical reporting.

**Project**: Retail Analytics Data Warehouse  
**ETL Tool**: SQL Server Integration Services (SSIS)  
**Load Strategy**: Incremental Load with Full Refresh Option  
**Execution Frequency**: Daily (scheduled for off-peak hours)

---

## Table of Contents

1. [ETL Architecture](#etl-architecture)
2. [Data Flow Overview](#data-flow-overview)
3. [Source System](#source-system)
4. [Staging Layer](#staging-layer)
5. [Dimension Loading Process](#dimension-loading-process)
6. [Fact Loading Process](#fact-loading-process)
7. [Master ETL Package](#master-etl-package)
8. [Error Handling & Logging](#error-handling--logging)
9. [Data Quality & Validation](#data-quality--validation)
10. [Performance Optimization](#performance-optimization)
11. [Scheduling & Orchestration](#scheduling--orchestration)
12. [Monitoring & Maintenance](#monitoring--maintenance)

---

## ETL Architecture

### High-Level Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEM (OLTP)                       │
│  ┌──────────┐  ┌─────────┐  ┌─────────┐  ┌──────────┐          │
│  │ customer │  │ product │  │ orders  │  │ supplier │           │
│  │          │  │         │  │         │  │          │           │
│  └──────────┘  └─────────┘  └─────────┘  └──────────┘           │
│       │             │             │             │                 │
└───────┼─────────────┼─────────────┼─────────────┼─────────────────┘
        │             │             │             │
        │    EXTRACT (SSIS OLE DB Source)        │
        │             │             │             │
        ▼             ▼             ▼             ▼
┌──────────────────────────────────────────────────────────────────┐
│                    STAGING AREA (Data Warehouse)                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐  │
│  │  STG_DimCustomer │  │  STG_DimProduct  │  │STG_DimSupplier│ │
│  └──────────────────┘  └──────────────────┘  └──────────────┘  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐  │
│  │ STG_DimCampaign  │  │STG_DimPayment    │  │STG_FactSales │  │
│  │                  │  │   Method         │  │              │  │
│  └──────────────────┘  └──────────────────┘  └──────────────┘  │
│  ┌──────────────────┐                                           │
│  │STG_FactReturns   │                                           │
│  │                  │                                           │
│  └──────────────────┘                                           │
└───────┬──────────────────┬──────────────────────┬───────────────┘
        │                  │                      │
        │    TRANSFORM (Data Cleansing, Lookups, SCD)
        │                  │                      │
        ▼                  ▼                      ▼
┌──────────────────────────────────────────────────────────────────┐
│              DATA WAREHOUSE (Star Schema)                         │
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    DIMENSION TABLES                         │ │
│  │  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐  │ │
│  │  │ DimCustomer │  │  DimProduct  │  │   DimSupplier   │  │ │
│  │  │   (SCD2)    │  │    (SCD2)    │  │     (SCD1)      │  │ │
│  │  └─────────────┘  └──────────────┘  └─────────────────┘  │ │
│  │  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐  │ │
│  │  │   DimDate   │  │ DimCampaign  │  │DimPaymentMethod │  │ │
│  │  │(Pre-loaded) │  │    (SCD1)    │  │     (SCD1)      │  │ │
│  │  └─────────────┘  └──────────────┘  └─────────────────┘  │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                      FACT TABLES                            │ │
│  │  ┌──────────────┐  ┌──────────────┐                        │ │
│  │  │  FactSales   │  │ FactReturns  │                        │ │
│  │  │              │  │              │                        │ │
│  │  └──────────────┘  └──────────────┘                        │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

### Technology Stack

- **ETL Tool**: SQL Server Integration Services (SSIS) 2019/2022
- **Source Database**: SQL Server (OLTP)
- **Target Database**: SQL Server (Data Warehouse)
- **Orchestration**: SQL Server Agent Jobs
- **Logging**: Custom ETL Control Tables + SSIS Logging
- **Error Handling**: Try-Catch blocks, Error Outputs, Email Notifications

---

## Data Flow Overview

### ETL Execution Order

The ETL process follows a strict execution order to maintain referential integrity:

```
1. Pre-Processing
   ├── Validate Connections
   ├── Check Source System Availability
   └── Truncate Staging Tables

2. Load Dimensions (Parallel where possible)
   ├── DimDate (one-time pre-load)
   ├── DimCustomer (SCD Type 2)
   ├── DimProduct (SCD Type 2)
   ├── DimSupplier (SCD Type 1)
   ├── DimPaymentMethod (SCD Type 1)
   └── DimCampaign (SCD Type 1)

3. Load Facts (Sequential - respects dependencies)
   ├── FactSales
   └── FactReturns (depends on FactSales for Order_Key)

4. Post-Processing
   ├── Update Statistics
   ├── Rebuild Indexes (if needed)
   ├── Data Quality Validation
   ├── Archive Old Staging Data
   └── Send Completion Notification
```

### Load Types

| Load Type | Description | Tables | Frequency |
|-----------|-------------|--------|-----------|
| **Full Load** | Complete refresh of all data | DimDate, DimPaymentMethod, DimSupplier, DimCampaign | Initial load or monthly |
| **Incremental Load** | Only new/modified records | DimCustomer, DimProduct, FactSales, FactReturns | Daily |
| **SCD Type 1** | Overwrite existing records | DimSupplier, DimPaymentMethod, DimCampaign | Daily |
| **SCD Type 2** | Historical tracking with versioning | DimCustomer, DimProduct | Daily |

---

## Source System

### Source Tables

| Source Table | Description | Key Columns | Row Count (Est.) |
|--------------|-------------|-------------|------------------|
| `customer` | Customer master data | customer_id, first_name, last_name, email | 50,000 |
| `product` | Product catalog | product_id, name, price, subcategory_id | 10,000 |
| `subcategory` | Product subcategories | subcategory_id, subcategory_name, category_id | 500 |
| `category` | Product categories | category_id, category_name | 50 |
| `supplier` | Supplier information | supplier_id, supplier_name, email | 200 |
| `orders` | Order headers | order_id, customer_id, order_date, campaign_id | 500,000 |
| `orderitem` | Order line items | orderitem_id, order_id, product_id, quantity | 2,000,000 |
| `returns` | Product returns | return_id, order_id, product_id, return_date | 50,000 |
| `payment_method` | Payment methods | payment_method_id, payment_method | 10 |
| `marketing_campaigns` | Marketing campaigns | campaign_id, campaign_name, offer_week | 100 |

### Source System Considerations

- **Change Detection**: Uses Modified_Date/Created_Date columns for incremental loads
- **Business Hours**: OLTP system is most active 8 AM - 6 PM
- **Batch Window**: ETL runs during off-peak hours (2 AM - 5 AM)
- **Data Retention**: Source system retains 3 years of transactional data

---

## Staging Layer

### Purpose of Staging

The staging area serves as an intermediary layer between source and target:

1. **Decouples** source and target systems
2. **Minimizes** lock time on source OLTP database
3. **Enables** complex transformations without impacting source
4. **Provides** restart points for failed loads
5. **Facilitates** data quality checks before loading to warehouse

### Staging Tables

```sql
-- Staging tables for dimensions
STG_DimCustomer
STG_DimProduct
STG_DimSupplier
STG_DimCampaign
STG_DimPaymentMethod

-- Staging tables for facts (mirror fact table structure but without surrogate keys)
STG_FactSales
STG_FactReturns
```

### Staging Process

```
┌─────────────────────────────────────────────────────────────┐
│ Step 1: Truncate Staging Tables                             │
│         Execute SQL Task: TRUNCATE TABLE STG_*              │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 2: Extract from Source                                 │
│         OLE DB Source → Raw data extraction                 │
│         Filter: WHERE Modified_Date >= @LastLoadDate        │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 3: Basic Transformations                               │
│         - Data type conversions                             │
│         - NULL handling                                     │
│         - Data cleansing (TRIM, UPPER, etc.)               │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 4: Load to Staging                                     │
│         OLE DB Destination → STG_ tables                    │
│         Fast Load option enabled                            │
└─────────────────────────────────────────────────────────────┘
```

### Staging Table Structures

#### STG_DimCustomer

```sql
CREATE TABLE STG_DimCustomer (
    Customer_ID INT NOT NULL,
    First_Name NVARCHAR(100),
    Last_Name NVARCHAR(100),
    Email NVARCHAR(255),
    Phone NVARCHAR(20),
    Date_of_Birth DATE,
    Gender NVARCHAR(10),
    City NVARCHAR(100),
    State NVARCHAR(100),
    Country NVARCHAR(100),
    Postal_Code NVARCHAR(20),
    Registration_Date DATE,
    Customer_Segment NVARCHAR(50),
    Modified_Date DATETIME,
    LoadDate DATETIME DEFAULT GETDATE()
);
```

#### STG_DimProduct

```sql
CREATE TABLE STG_DimProduct (
    Product_ID INT NOT NULL,
    Product_Name NVARCHAR(255),
    Product_Description NVARCHAR(MAX),
    Subcategory_Name NVARCHAR(100),
    Category_Name NVARCHAR(100),
    Supplier_ID INT,
    Supplier_Name NVARCHAR(255),
    Unit_Price DECIMAL(10,2),
    Unit_Cost DECIMAL(10,2),
    Product_Size NVARCHAR(50),
    Product_Color NVARCHAR(50),
    Is_Active BIT,
    Modified_Date DATETIME,
    LoadDate DATETIME DEFAULT GETDATE()
);
```

#### STG_DimSupplier

```sql
CREATE TABLE STG_DimSupplier (
    Supplier_ID INT NOT NULL,
    Supplier_Name NVARCHAR(255),
    Contact_Name NVARCHAR(100),
    Email NVARCHAR(255),
    Phone NVARCHAR(20),
    Address NVARCHAR(255),
    City NVARCHAR(100),
    Country NVARCHAR(100),
    Modified_Date DATETIME,
    LoadDate DATETIME DEFAULT GETDATE()
);
```

#### STG_DimCampaign

```sql
CREATE TABLE STG_DimCampaign (
    Campaign_ID INT NOT NULL,
    Campaign_Name NVARCHAR(255),
    Campaign_Type NVARCHAR(100),
    Start_Date DATE,
    End_Date DATE,
    Discount_Percentage DECIMAL(5,2),
    Budget DECIMAL(15,2),
    Modified_Date DATETIME,
    LoadDate DATETIME DEFAULT GETDATE()
);
```

#### STG_DimPaymentMethod

```sql
CREATE TABLE STG_DimPaymentMethod (
    Payment_Method_ID INT NOT NULL,
    Payment_Method NVARCHAR(50),
    Processing_Fee_Percentage DECIMAL(5,2),
    Is_Active BIT,
    Modified_Date DATETIME,
    LoadDate DATETIME DEFAULT GETDATE()
);
```

#### STG_FactSales

```sql
CREATE TABLE STG_FactSales (
    Order_ID INT,
    OrderItem_ID INT,
    Customer_ID INT,
    Product_ID INT,
    Campaign_ID INT,
    Payment_Method_ID INT,
    Order_Date DATE,
    Ship_Date DATE,
    Quantity INT,
    Unit_Price DECIMAL(10,2),
    Unit_Cost DECIMAL(10,2),
    Discount_Amount DECIMAL(10,2),
    Tax_Amount DECIMAL(10,2),
    Shipping_Cost DECIMAL(10,2),
    LoadDate DATETIME DEFAULT GETDATE()
);
```

#### STG_FactReturns

```sql
CREATE TABLE STG_FactReturns (
    Return_ID INT,
    Order_ID INT,
    OrderItem_ID INT,
    Product_ID INT,
    Customer_ID INT,
    Return_Date DATE,
    Return_Quantity INT,
    Return_Amount DECIMAL(10,2),
    Return_Reason NVARCHAR(255),
    Refund_Amount DECIMAL(10,2),
    Restocking_Fee DECIMAL(10,2),
    LoadDate DATETIME DEFAULT GETDATE()
);
```

---

## Dimension Loading Process

### General Dimension Load Pattern

All dimension loads follow this pattern:

```
1. Extract from Source to Staging
2. Transform & Cleanse in Staging
3. Lookup Existing Records
4. Apply SCD Logic (Type 1 or Type 2)
5. Load New/Updated Records
6. Log Results
```

### DimDate - Pre-Population

**Load Type**: One-time full load  
**SCD Type**: N/A (static reference data)

```sql
-- Generate date dimension for 10 years
DECLARE @StartDate DATE = '2020-01-01';
DECLARE @EndDate DATE = '2030-12-31';

WITH DateSequence AS (
    SELECT @StartDate AS DateValue
    UNION ALL
    SELECT DATEADD(DAY, 1, DateValue)
    FROM DateSequence
    WHERE DateValue < @EndDate
)
INSERT INTO DimDate (
    Date_Key, Full_Date, Day_of_Week, Day_Name, 
    Day_of_Month, Day_of_Year, Week_of_Year, 
    Month_Number, Month_Name, Quarter, Year, 
    Is_Weekend, Is_Holiday
)
SELECT 
    CONVERT(INT, FORMAT(DateValue, 'yyyyMMdd')) AS Date_Key,
    DateValue AS Full_Date,
    DATEPART(WEEKDAY, DateValue) AS Day_of_Week,
    DATENAME(WEEKDAY, DateValue) AS Day_Name,
    DAY(DateValue) AS Day_of_Month,
    DATEPART(DAYOFYEAR, DateValue) AS Day_of_Year,
    DATEPART(WEEK, DateValue) AS Week_of_Year,
    MONTH(DateValue) AS Month_Number,
    DATENAME(MONTH, DateValue) AS Month_Name,
    DATEPART(QUARTER, DateValue) AS Quarter,
    YEAR(DateValue) AS Year,
    CASE WHEN DATEPART(WEEKDAY, DateValue) IN (1,7) THEN 1 ELSE 0 END AS Is_Weekend,
    0 AS Is_Holiday -- Can be updated separately with holiday logic
FROM DateSequence
OPTION (MAXRECURSION 0);
```

### DimCustomer - SCD Type 2 with Staging

**Load Type**: Incremental  
**SCD Type**: Type 2 (Track historical changes)

**SSIS Package Flow**:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Truncate STG_DimCustomer                                 │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Extract to Staging                                       │
│    Source: customer (OLTP)                                  │
│    Query:                                                   │
│      SELECT c.customer_id, c.first_name, c.last_name,      │
│             c.email, c.phone, c.date_of_birth, c.gender,   │
│             c.city, c.state, c.country, c.postal_code,     │
│             c.registration_date, c.customer_segment,       │
│             c.modified_date                                │
│      FROM customer c                                        │
│      WHERE c.modified_date >= ?                            │
│    Destination: STG_DimCustomer                            │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Data Cleansing in Staging                                │
│    - Derived Column: TRIM(First_Name), TRIM(Last_Name)     │
│    - Derived Column: UPPER(Email)                          │
│    - Conditional Split: Filter invalid emails               │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. SCD Type 2 Processing from Staging                       │
│    Source: STG_DimCustomer                                  │
│    SCD Transformation:                                      │
│      - Compare with existing DimCustomer records            │
│      - Identify: New, Changed, Unchanged                    │
│      - Business Key: Customer_ID                            │
│      - Changing Attributes: All except Customer_ID          │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Update/Insert Dimension                                  │
│    New Records → INSERT with Is_Current = 1                │
│    Changed Records → Update old (Is_Current = 0),          │
│                      Insert new (Is_Current = 1)           │
└─────────────────────────────────────────────────────────────┘
```

**SCD Type 2 Logic**:

```sql
-- For changed records, close the old version
UPDATE DimCustomer
SET 
    End_Date = GETDATE(),
    Is_Current = 0
WHERE Customer_ID IN (
    SELECT s.Customer_ID
    FROM STG_DimCustomer s
    INNER JOIN DimCustomer d ON s.Customer_ID = d.Customer_ID
    WHERE d.Is_Current = 1
    AND (
        s.Email <> d.Email OR
        s.Phone <> d.Phone OR
        s.City <> d.City OR
        s.State <> d.State OR
        s.Customer_Segment <> d.Customer_Segment
    )
)
AND Is_Current = 1;

-- Insert new versions for changed records
INSERT INTO DimCustomer (
    Customer_ID, First_Name, Last_Name, Email, Phone,
    Date_of_Birth, Gender, City, State, Country, Postal_Code,
    Registration_Date, Customer_Segment, Start_Date, End_Date, Is_Current
)
SELECT 
    s.Customer_ID, s.First_Name, s.Last_Name, s.Email, s.Phone,
    s.Date_of_Birth, s.Gender, s.City, s.State, s.Country, s.Postal_Code,
    s.Registration_Date, s.Customer_Segment, 
    GETDATE() AS Start_Date,
    '9999-12-31' AS End_Date,
    1 AS Is_Current
FROM STG_DimCustomer s
WHERE EXISTS (
    SELECT 1 FROM DimCustomer d
    WHERE d.Customer_ID = s.Customer_ID
    AND d.End_Date = GETDATE()  -- Just closed
);

-- Insert completely new customers
INSERT INTO DimCustomer (
    Customer_ID, First_Name, Last_Name, Email, Phone,
    Date_of_Birth, Gender, City, State, Country, Postal_Code,
    Registration_Date, Customer_Segment, Start_Date, End_Date, Is_Current
)
SELECT 
    s.Customer_ID, s.First_Name, s.Last_Name, s.Email, s.Phone,
    s.Date_of_Birth, s.Gender, s.City, s.State, s.Country, s.Postal_Code,
    s.Registration_Date, s.Customer_Segment,
    GETDATE() AS Start_Date,
    '9999-12-31' AS End_Date,
    1 AS Is_Current
FROM STG_DimCustomer s
WHERE NOT EXISTS (
    SELECT 1 FROM DimCustomer d
    WHERE d.Customer_ID = s.Customer_ID
);
```

### DimProduct - SCD Type 2 with Staging

**Load Type**: Incremental  
**SCD Type**: Type 2 (Track price and supplier changes)

**SSIS Package Flow**:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Truncate STG_DimProduct                                  │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Extract to Staging (Multi-table JOIN)                    │
│    Query:                                                   │
│      SELECT p.product_id, p.name, p.description,           │
│             sc.subcategory_name, c.category_name,          │
│             s.supplier_id, s.supplier_name,                │
│             p.unit_price, p.unit_cost, p.size, p.color,    │
│             p.is_active, p.modified_date                   │
│      FROM product p                                         │
│      JOIN subcategory sc ON p.subcategory_id = sc.id       │
│      JOIN category c ON sc.category_id = c.category_id     │
│      LEFT JOIN supplier s ON p.supplier_id = s.supplier_id │
│      WHERE p.modified_date >= ?                            │
│    Destination: STG_DimProduct                             │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Data Transformations in Staging                          │
│    - Derived Column: Calculate Profit_Margin                │
│    - Conditional Split: Filter inactive products            │
│    - Data Conversion: Ensure proper types                   │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. SCD Type 2 Processing from Staging                       │
│    Business Key: Product_ID                                 │
│    Changing Attributes: Unit_Price, Unit_Cost, Supplier_ID  │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Load to DimProduct                                       │
└─────────────────────────────────────────────────────────────┘
```

**Key Transformations**:

```sql
-- Derived Column for Profit Margin
(Unit_Price - Unit_Cost) / NULLIF(Unit_Price, 0) * 100
```

### DimSupplier - SCD Type 1 with Staging

**Load Type**: Full refresh  
**SCD Type**: Type 1 (Overwrite changes)

**SSIS Package Flow**:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Truncate STG_DimSupplier                                 │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Extract to Staging                                       │
│    Source: supplier (OLTP)                                  │
│    Query: SELECT * FROM supplier                            │
│    Destination: STG_DimSupplier                            │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Data Cleansing in Staging                                │
│    - TRIM and UPPER transformations                         │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Merge from Staging to Dimension                          │
│    MERGE DimSupplier USING STG_DimSupplier                  │
│    ON Supplier_ID = Supplier_ID                             │
│    WHEN MATCHED → UPDATE                                    │
│    WHEN NOT MATCHED → INSERT                                │
└─────────────────────────────────────────────────────────────┘
```

**Merge Logic**:

```sql
MERGE INTO DimSupplier AS target
USING STG_DimSupplier AS source
ON target.Supplier_ID = source.Supplier_ID
WHEN MATCHED THEN
    UPDATE SET
        target.Supplier_Name = source.Supplier_Name,
        target.Contact_Name = source.Contact_Name,
        target.Email = source.Email,
        target.Phone = source.Phone,
        target.Address = source.Address,
        target.City = source.City,
        target.Country = source.Country,
        target.Modified_Date = source.Modified_Date
WHEN NOT MATCHED BY TARGET THEN
    INSERT (Supplier_ID, Supplier_Name, Contact_Name, Email, Phone, 
            Address, City, Country, Modified_Date)
    VALUES (source.Supplier_ID, source.Supplier_Name, source.Contact_Name, 
            source.Email, source.Phone, source.Address, source.City, 
            source.Country, source.Modified_Date);
```

### DimPaymentMethod - SCD Type 1 with Staging

**Load Type**: Full refresh  
**SCD Type**: Type 1

**SSIS Package Flow**:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Truncate STG_DimPaymentMethod                            │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Extract to Staging                                       │
│    Source: payment_method (OLTP)                            │
│    Destination: STG_DimPaymentMethod                        │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Merge from Staging to Dimension                          │
└─────────────────────────────────────────────────────────────┘
```

**Merge Logic**:

```sql
MERGE INTO DimPaymentMethod AS target
USING STG_DimPaymentMethod AS source
ON target.Payment_Method_ID = source.Payment_Method_ID
WHEN MATCHED THEN
    UPDATE SET
        target.Payment_Method = source.Payment_Method,
        target.Processing_Fee_Percentage = source.Processing_Fee_Percentage,
        target.Is_Active = source.Is_Active
WHEN NOT MATCHED BY TARGET THEN
    INSERT (Payment_Method_ID, Payment_Method, Processing_Fee_Percentage, Is_Active)
    VALUES (source.Payment_Method_ID, source.Payment_Method, 
            source.Processing_Fee_Percentage, source.Is_Active);
```

### DimCampaign - SCD Type 1 with Staging

**Load Type**: Incremental  
**SCD Type**: Type 1

**SSIS Package Flow**:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Truncate STG_DimCampaign                                 │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Extract to Staging                                       │
│    Source: marketing_campaigns (OLTP)                       │
│    Filter: WHERE modified_date >= @LastLoadDate            │
│    Destination: STG_DimCampaign                            │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Calculate Campaign Duration in Staging                   │
│    Derived Column: DATEDIFF(day, Start_Date, End_Date)     │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Merge from Staging to Dimension                          │
└─────────────────────────────────────────────────────────────┘
```

**Merge Logic**:

```sql
MERGE INTO DimCampaign AS target
USING STG_DimCampaign AS source
ON target.Campaign_ID = source.Campaign_ID
WHEN MATCHED THEN
    UPDATE SET
        target.Campaign_Name = source.Campaign_Name,
        target.Campaign_Type = source.Campaign_Type,
        target.Start_Date = source.Start_Date,
        target.End_Date = source.End_Date,
        target.Discount_Percentage = source.Discount_Percentage,
        target.Budget = source.Budget
WHEN NOT MATCHED BY TARGET THEN
    INSERT (Campaign_ID, Campaign_Name, Campaign_Type, Start_Date, 
            End_Date, Discount_Percentage, Budget)
    VALUES (source.Campaign_ID, source.Campaign_Name, source.Campaign_Type, 
            source.Start_Date, source.End_Date, source.Discount_Percentage, 
            source.Budget);
```

---

## Fact Loading Process

### FactSales - Transaction Fact Table

**Load Type**: Incremental  
**Granularity**: One row per order line item

**SSIS Package Flow**:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Truncate STG_FactSales                                   │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Extract to Staging                                       │
│    Source Query (Multi-table JOIN):                         │
│      SELECT oi.orderitem_id, oi.order_id,                  │
│             o.customer_id, oi.product_id,                  │
│             o.campaign_id, o.payment_method_id,            │
│             o.order_date, o.ship_date,                     │
│             oi.quantity, oi.unit_price, oi.unit_cost,      │
│             oi.discount_amount, o.tax_amount,              │
│             o.shipping_cost                                 │
│      FROM orderitem oi                                      │
│      INNER JOIN orders o ON oi.order_id = o.order_id       │
│      WHERE o.order_date >= @LastLoadDate                   │
│    Destination: STG_FactSales                              │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Calculate Derived Measures in Staging                    │
│    - Sales_Amount = Quantity * Unit_Price                   │
│    - Cost_Amount = Quantity * Unit_Cost                     │
│    - Gross_Profit = Sales_Amount - Cost_Amount             │
│    - Net_Sales = Sales_Amount - Discount_Amount            │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Dimension Key Lookups from Staging                       │
│    Lookup Transformations:                                  │
│      Customer_Key ← DimCustomer (Customer_ID, Is_Current=1) │
│      Product_Key ← DimProduct (Product_ID, Is_Current=1)    │
│      Campaign_Key ← DimCampaign (Campaign_ID)              │
│      PaymentMethod_Key ← DimPaymentMethod (Payment_ID)     │
│      Order_Date_Key ← DimDate (Order_Date)                 │
│      Ship_Date_Key ← DimDate (Ship_Date)                   │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Error Handling                                           │
│    Conditional Split:                                       │
│      - Valid Lookups → Continue to FactSales               │
│      - Failed Lookups → Error Output (Log & Alert)         │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. Load to FactSales                                        │
│    OLE DB Destination (Fast Load)                           │
└─────────────────────────────────────────────────────────────┘
```

**Key Transformations**:

```sql
-- Derived Columns
Sales_Amount = Quantity * Unit_Price
Cost_Amount = Quantity * Unit_Cost
Gross_Profit = (Quantity * Unit_Price) - (Quantity * Unit_Cost)
Net_Sales = (Quantity * Unit_Price) - Discount_Amount
Profit_Margin = CASE 
    WHEN (Quantity * Unit_Price) > 0 
    THEN ((Quantity * Unit_Price) - (Quantity * Unit_Cost)) / (Quantity * Unit_Price) * 100
    ELSE 0 
END
```

**Lookup Queries**:

```sql
-- Customer Lookup (SCD Type 2 aware)
SELECT Customer_Key 
FROM DimCustomer 
WHERE Customer_ID = ? 
  AND Is_Current = 1

-- Product Lookup (SCD Type 2 aware)
SELECT Product_Key 
FROM DimProduct 
WHERE Product_ID = ? 
  AND Is_Current = 1
  
-- Date Lookup
SELECT Date_Key 
FROM DimDate 
WHERE Full_Date = ?
```

**Error Handling**:

```sql
-- Log failed lookups
INSERT INTO ETL_ErrorLog (
    BatchID, PackageName, ErrorTime, 
    ErrorType, ErrorDescription, SourceRow
)
SELECT 
    @BatchID,
    'FactSales_Load',
    GETDATE(),
    'Lookup Failure',
    'Customer_ID not found in DimCustomer: ' + CAST(Customer_ID AS VARCHAR),
    'Order_ID: ' + CAST(Order_ID AS VARCHAR)
FROM STG_FactSales s
WHERE NOT EXISTS (
    SELECT 1 FROM DimCustomer d 
    WHERE d.Customer_ID = s.Customer_ID 
    AND d.Is_Current = 1
);
```

### FactReturns - Transaction Fact Table

**Load Type**: Incremental  
**Granularity**: One row per return transaction  
**Dependencies**: Requires FactSales to be loaded first

**SSIS Package Flow**:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Truncate STG_FactReturns                                 │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Extract to Staging                                       │
│    Source Query:                                            │
│      SELECT r.return_id, r.order_id, r.orderitem_id,       │
│             r.product_id, r.customer_id, r.return_date,    │
│             r.return_quantity, r.return_amount,            │
│             r.return_reason, r.refund_amount,              │
│             r.restocking_fee                               │
│      FROM returns r                                         │
│      WHERE r.return_date >= @LastLoadDate                  │
│    Destination: STG_FactReturns                            │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Dimension Key Lookups from Staging                       │
│    Lookups:                                                 │
│      Customer_Key ← DimCustomer                            │
│      Product_Key ← DimProduct                              │
│      Return_Date_Key ← DimDate                             │
│      Order_Key ← FactSales (Order_ID + OrderItem_ID)      │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Calculate Return Metrics in Staging                      │
│    - Net_Refund_Amount = Refund_Amount - Restocking_Fee    │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Load to FactReturns                                      │
└─────────────────────────────────────────────────────────────┘
```

**Critical Lookup**:

```sql
-- Lookup original order from FactSales
SELECT Order_Key 
FROM FactSales 
WHERE Order_ID = ? 
  AND OrderItem_ID = ?
```

This allows tracking which sale was returned.

---

## Master ETL Package

### Package Architecture

**Master Package**: `Master_DW_ETL.dtsx`

```
Master_DW_ETL
├── Sequence Container: Pre-Processing
│   ├── Execute SQL: Validate Connections
│   ├── Execute SQL: Check Source Availability
│   └── Execute SQL: Truncate All Staging Tables
│
├── Sequence Container: Load Dimensions (Parallel)
│   ├── Execute Package: Load_DimCustomer.dtsx
│   ├── Execute Package: Load_DimProduct.dtsx
│   ├── Execute Package: Load_DimSupplier.dtsx
│   ├── Execute Package: Load_DimPaymentMethod.dtsx
│   └── Execute Package: Load_DimCampaign.dtsx
│
├── Sequence Container: Load Facts (Sequential)
│   ├── Execute Package: Load_FactSales.dtsx
│   └── Execute Package: Load_FactReturns.dtsx
│
└── Sequence Container: Post-Processing
    ├── Execute SQL: Update Statistics
    ├── Execute SQL: Rebuild Fragmented Indexes
    ├── Execute SQL: Data Quality Checks
    ├── Execute SQL: Archive Staging Data
    └── Send Mail: Success Notification
```

### Package Configuration

**Variables**:

```
User::LastLoadDate         (DateTime) - Tracks last successful load
User::BatchID               (Int32)    - Unique batch identifier
User::SourceServer          (String)   - OLTP server connection
User::TargetServer          (String)   - DW server connection
User::ErrorThreshold        (Int32)    - Max errors before abort
```

**Connection Managers**:

```
SourceOLTP    → SQL Server connection to transactional DB
TargetDW      → SQL Server connection to data warehouse
EmailSMTP     → SMTP connection for notifications
```

### Execution Control

**Control Table**:

```sql
CREATE TABLE ETL_ControlTable (
    ControlKey VARCHAR(50) PRIMARY KEY,
    ControlValue VARCHAR(255),
    LastUpdated DATETIME DEFAULT GETDATE()
);

-- Seed values
INSERT INTO ETL_ControlTable (ControlKey, ControlValue)
VALUES 
    ('LastSuccessfulLoad', '2024-01-01 00:00:00'),
    ('BatchID', '0'),
    ('IsETLRunning', '0');
```

**Load Date Management**:

```sql
-- At start of ETL
DECLARE @LastLoadDate DATETIME;
SELECT @LastLoadDate = CONVERT(DATETIME, ControlValue)
FROM ETL_ControlTable
WHERE ControlKey = 'LastSuccessfulLoad';

-- At end of successful ETL
UPDATE ETL_ControlTable
SET ControlValue = CONVERT(VARCHAR(20), GETDATE(), 120),
    LastUpdated = GETDATE()
WHERE ControlKey = 'LastSuccessfulLoad';
```

### Master Package Script

**Execute SQL Task - Start of ETL**:

```sql
DECLARE @IsRunning INT;

-- Check if ETL is already running
SELECT @IsRunning = CAST(ControlValue AS INT)
FROM ETL_ControlTable
WHERE ControlKey = 'IsETLRunning';

IF @IsRunning = 1
BEGIN
    RAISERROR('ETL is already running. Aborting.', 16, 1);
    RETURN;
END

-- Set running flag
UPDATE ETL_ControlTable
SET ControlValue = '1'
WHERE ControlKey = 'IsETLRunning';

-- Truncate all staging tables
TRUNCATE TABLE STG_DimCustomer;
TRUNCATE TABLE STG_DimProduct;
TRUNCATE TABLE STG_DimSupplier;
TRUNCATE TABLE STG_DimCampaign;
TRUNCATE TABLE STG_DimPaymentMethod;
TRUNCATE TABLE STG_FactSales;
TRUNCATE TABLE STG_FactReturns;
```

**Execute SQL Task - End of ETL**:

```sql
-- Clear running flag
UPDATE ETL_ControlTable
SET ControlValue = '0'
WHERE ControlKey = 'IsETLRunning';

-- Update last load date
UPDATE ETL_ControlTable
SET ControlValue = CONVERT(VARCHAR(20), GETDATE(), 120)
WHERE ControlKey = 'LastSuccessfulLoad';
```

---

## Error Handling & Logging

### Error Logging Framework

**ETL_ExecutionLog Table**:

```sql
CREATE TABLE ETL_ExecutionLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT,
    PackageName VARCHAR(255),
    StartTime DATETIME,
    EndTime DATETIME,
    Status VARCHAR(50), -- Running, Success, Failed
    Duration_Seconds INT,
    RowsRead INT,
    RowsWritten INT,
    RowsError INT,
    ErrorMessage VARCHAR(MAX)
);
```

**ETL_ErrorLog Table**:

```sql
CREATE TABLE ETL_ErrorLog (
    ErrorID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT,
    PackageName VARCHAR(255),
    ErrorTime DATETIME DEFAULT GETDATE(),
    ErrorType VARCHAR(100), -- Lookup Failure, Data Quality, Transformation
    ErrorDescription VARCHAR(MAX),
    SourceRow VARCHAR(MAX),
    RowCount INT DEFAULT 1
);
```

### SSIS Error Handling Pattern

**Event Handlers**:

```
OnError Event:
  ├── Execute SQL: Log Error to ETL_ErrorLog
  ├── Script Task: Format error email
  └── Send Mail: Alert DBA team

OnWarning Event:
  └── Execute SQL: Log Warning to ETL_WarningLog

OnPreExecute Event:
  └── Execute SQL: Insert into ETL_ExecutionLog (Status = 'Running')

OnPostExecute Event:
  └── Execute SQL: Update ETL_ExecutionLog (Status = 'Success', EndTime, RowCounts)
```

**Error Output Routing**:

```
OLE DB Source
  └── Lookup Transformation
      ├── Match Output → Continue to destination
      └── Error Output → 
          ├── Script Component: Format error details
          └── OLE DB Destination: ETL_ErrorLog
```

### Logging Stored Procedure

```sql
CREATE PROCEDURE usp_LogETLExecution
    @BatchID INT,
    @PackageName VARCHAR(255),
    @Status VARCHAR(50),
    @RowsRead INT = NULL,
    @RowsWritten INT = NULL,
    @RowsError INT = NULL,
    @ErrorMessage VARCHAR(MAX) = NULL
AS
BEGIN
    IF @Status = 'Start'
    BEGIN
        INSERT INTO ETL_ExecutionLog (
            BatchID, PackageName, StartTime, Status
        )
        VALUES (
            @BatchID, @PackageName, GETDATE(), 'Running'
        );
    END
    ELSE
    BEGIN
        UPDATE ETL_ExecutionLog
        SET 
            EndTime = GETDATE(),
            Status = @Status,
            Duration_Seconds = DATEDIFF(SECOND, StartTime, GETDATE()),
            RowsRead = @RowsRead,
            RowsWritten = @RowsWritten,
            RowsError = @RowsError,
            ErrorMessage = @ErrorMessage
        WHERE BatchID = @BatchID 
          AND PackageName = @PackageName
          AND EndTime IS NULL;
    END
END;
```

---

## Data Quality & Validation

### Pre-Load Validation

**Source Data Quality Checks**:

```sql
-- Check for orphaned records in orders
SELECT COUNT(*) AS OrphanedOrders
FROM orders o
LEFT JOIN customer c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Check for invalid dates
SELECT COUNT(*) AS InvalidDates
FROM orders
WHERE order_date > GETDATE() 
   OR ship_date < order_date;

-- Check for negative quantities
SELECT COUNT(*) AS NegativeQuantity
FROM orderitem
WHERE quantity <= 0 OR unit_price < 0;
```

### Post-Load Validation

**Reconciliation Queries**:

```sql
-- FactSales row count vs source
DECLARE @SourceCount INT, @TargetCount INT;

SELECT @SourceCount = COUNT(*)
FROM source_db.dbo.orderitem
WHERE order_date >= @LastLoadDate;

SELECT @TargetCount = COUNT(*)
FROM FactSales
WHERE Order_Date_Key >= @LastLoadDateKey;

IF ABS(@SourceCount - @TargetCount) > (@SourceCount * 0.05)
BEGIN
    RAISERROR('Row count variance exceeds 5%', 16, 1);
END
```

**Referential Integrity Checks**:

```sql
-- Check for orphaned fact records
SELECT COUNT(*) AS OrphanedSales
FROM FactSales f
LEFT JOIN DimCustomer c ON f.Customer_Key = c.Customer_Key
WHERE c.Customer_Key IS NULL;

-- Verify SCD2 integrity
SELECT Customer_ID, COUNT(*) AS ActiveCount
FROM DimCustomer
WHERE Is_Current = 1
GROUP BY Customer_ID
HAVING COUNT(*) > 1; -- Should be 0
```

### Data Quality Metrics

```sql
-- Calculate DQ score
CREATE VIEW vw_DataQualityMetrics AS
SELECT 
    CAST(GETDATE() AS DATE) AS MetricDate,
    'FactSales' AS TableName,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN Customer_Key = -1 THEN 1 ELSE 0 END) AS MissingCustomers,
    SUM(CASE WHEN Product_Key = -1 THEN 1 ELSE 0 END) AS MissingProducts,
    SUM(CASE WHEN Quantity <= 0 THEN 1 ELSE 0 END) AS InvalidQuantity,
    (1.0 - (
        SUM(CASE WHEN Customer_Key = -1 OR Product_Key = -1 OR Quantity <= 0 THEN 1 ELSE 0 END) * 1.0 
        / COUNT(*)
    )) * 100 AS DataQualityScore
FROM FactSales
WHERE Order_Date_Key >= CONVERT(INT, FORMAT(GETDATE(), 'yyyyMMdd'));
```

---

## Performance Optimization

### Indexing Strategy

**Dimension Tables**:

```sql
-- DimCustomer
CREATE UNIQUE CLUSTERED INDEX IX_DimCustomer_CustomerKey 
ON DimCustomer(Customer_Key);

CREATE NONCLUSTERED INDEX IX_DimCustomer_BusinessKey 
ON DimCustomer(Customer_ID, Is_Current) 
INCLUDE (Customer_Key);

CREATE NONCLUSTERED INDEX IX_DimCustomer_Dates 
ON DimCustomer(Start_Date, End_Date, Is_Current);

-- DimProduct
CREATE UNIQUE CLUSTERED INDEX IX_DimProduct_ProductKey 
ON DimProduct(Product_Key);

CREATE NONCLUSTERED INDEX IX_DimProduct_BusinessKey 
ON DimProduct(Product_ID, Is_Current) 
INCLUDE (Product_Key);
```

**Fact Tables**:

```sql
-- FactSales
CREATE CLUSTERED COLUMNSTORE INDEX CCIX_FactSales 
ON FactSales;

-- Additional nonclustered for common query patterns
CREATE NONCLUSTERED INDEX IX_FactSales_OrderDate 
ON FactSales(Order_Date_Key) 
INCLUDE (Customer_Key, Product_Key, Sales_Amount);

CREATE NONCLUSTERED INDEX IX_FactSales_Customer 
ON FactSales(Customer_Key, Order_Date_Key);
```

### Partitioning

**FactSales Partitioning by Year**:

```sql
-- Create partition function
CREATE PARTITION FUNCTION PF_FactSales_Year (INT)
AS RANGE RIGHT FOR VALUES (20200101, 20210101, 20220101, 20230101, 20240101);

-- Create partition scheme
CREATE PARTITION SCHEME PS_FactSales_Year
AS PARTITION PF_FactSales_Year
ALL TO ([PRIMARY]);

-- Create partitioned table
CREATE TABLE FactSales (
    Order_Key INT IDENTITY(1,1),
    Order_Date_Key INT NOT NULL,
    -- ... other columns
) ON PS_FactSales_Year(Order_Date_Key);
```

### SSIS Performance Tuning

**Buffer Settings**:

```xml
<BufferSize>10485760</BufferSize>  <!-- 10 MB -->
<MaximumBuffers>100</MaximumBuffers>
<RowsPerBuffer>10000</RowsPerBuffer>
```

**Parallel Execution**:

- Set `MaxConcurrentExecutables` to number of logical processors
- Use parallel sequence containers for independent dimension loads
- Limit parallelism for fact loads to respect dependencies

**Bulk Insert Options**:

```
Table Lock: TRUE
Check Constraints: FALSE
Rows per Batch: 10000
Maximum Insert Commit Size: 500000
```

---

## Scheduling & Orchestration

### SQL Server Agent Job

**Job Name**: `DW_DailyETL`

**Job Steps**:

```
Step 1: Pre-Validation
  Type: T-SQL
  Command: EXEC usp_PreETLValidation
  On Success: Go to Step 2
  On Failure: Quit with failure

Step 2: Execute SSIS Package
  Type: SSIS Package
  Package: Master_DW_ETL.dtsx
  On Success: Go to Step 3
  On Failure: Go to Step 5 (Error notification)

Step 3: Post-Validation
  Type: T-SQL
  Command: EXEC usp_PostETLValidation
  On Success: Go to Step 4
  On Failure: Go to Step 5

Step 4: Success Notification
  Type: T-SQL
  Command: EXEC msdb.dbo.sp_send_dbmail @recipients='team@company.com'...
  On Success: Quit with success

Step 5: Failure Notification
  Type: T-SQL
  Command: EXEC msdb.dbo.sp_send_dbmail @recipients='dba@company.com'...
  On Success: Quit with failure
```

**Schedule**:

```
Frequency: Daily
Start Time: 02:00 AM
Duration: Maximum 3 hours
Retry Attempts: 2
Retry Interval: 15 minutes
```

### Orchestration Script

```sql
CREATE PROCEDURE usp_ExecuteETL
AS
BEGIN
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @BatchID INT;
    
    -- Generate Batch ID
    SELECT @BatchID = ISNULL(MAX(BatchID), 0) + 1 
    FROM ETL_ExecutionLog;
    
    -- Log Start
    INSERT INTO ETL_ExecutionLog 
        (BatchID, PackageName, StartTime, Status)
    VALUES 
        (@BatchID, 'Master_ETL_Load', @StartTime, 'Running');
    
    -- Execute SSIS Package
    EXEC sp_start_job @job_name = 'DW_DailyETL';
    
    -- Wait for completion (polling logic)
    -- Update ETL_ExecutionLog with end time and status
END;
```

### Dependency Management

**Precedence Constraints**:
- All dimensions must complete before any facts start
- FactReturns depends on FactSales completion
- Post-processing waits for all fact loads

**Handling Failures**:
- Dimension failure: Abort entire ETL (referential integrity)
- Fact failure: Log error, continue with other facts (configurable)
- Post-processing failure: Log warning, send alert

---

## Monitoring & Maintenance

### Real-Time Monitoring

**SSISDB Catalog Views**:

```sql
-- Check currently running packages
SELECT 
    execution_id,
    package_name,
    status,
    start_time,
    DATEDIFF(MINUTE, start_time, GETDATE()) AS elapsed_minutes
FROM catalog.executions
WHERE status = 2 -- Running
ORDER BY start_time DESC;

-- View execution messages (errors/warnings)
SELECT 
    em.message_time,
    em.message_type,
    em.message,
    ex.package_name
FROM catalog.event_messages em
INNER JOIN catalog.executions ex ON em.operation_id = ex.execution_id
WHERE ex.execution_id = <execution_id>
    AND em.message_type IN (110, 120) -- Errors and Warnings
ORDER BY em.message_time DESC;
```

**Custom Monitoring Dashboard**:

```sql
-- ETL Performance Trend (Last 30 Days)
SELECT 
    CAST(StartTime AS DATE) AS LoadDate,
    PackageName,
    AVG(Duration_Seconds) AS AvgDuration,
    SUM(RowsWritten) AS TotalRows,
    SUM(RowsError) AS TotalErrors
FROM ETL_ExecutionLog
WHERE StartTime >= DATEADD(DAY, -30, GETDATE())
    AND Status = 'Success'
GROUP BY CAST(StartTime AS DATE), PackageName
ORDER BY LoadDate DESC, PackageName;

-- Error Trend Analysis
SELECT 
    CAST(ErrorTime AS DATE) AS ErrorDate,
    PackageName,
    ErrorType,
    COUNT(*) AS ErrorCount
FROM ETL_ErrorLog
WHERE ErrorTime >= DATEADD(DAY, -7, GETDATE())
GROUP BY CAST(ErrorTime AS DATE), PackageName, ErrorType
ORDER BY ErrorDate DESC, ErrorCount DESC;
```

### Maintenance Tasks

#### Weekly Maintenance

```sql
-- 1. Update statistics on all tables
EXEC sp_updatestats;

-- 2. Rebuild fragmented indexes (> 30% fragmentation)
DECLARE @TableName NVARCHAR(255);
DECLARE @IndexName NVARCHAR(255);
DECLARE @SQL NVARCHAR(MAX);

DECLARE cur CURSOR FOR
SELECT 
    OBJECT_NAME(ips.object_id) AS TableName,
    i.name AS IndexName
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
INNER JOIN sys.indexes i ON ips.object_id = i.object_id 
    AND ips.index_id = i.index_id
WHERE ips.avg_fragmentation_in_percent > 30
    AND ips.page_count > 1000;

OPEN cur;
FETCH NEXT FROM cur INTO @TableName, @IndexName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = 'ALTER INDEX ' + @IndexName + ' ON ' + @TableName + ' REBUILD;';
    EXEC sp_executesql @SQL;
    FETCH NEXT FROM cur INTO @TableName, @IndexName;
END;

CLOSE cur;
DEALLOCATE cur;
```

#### Monthly Maintenance

```sql
-- 1. Archive old ETL logs (> 90 days)
DELETE FROM ETL_ErrorLog 
WHERE ErrorTime < DATEADD(DAY, -90, GETDATE());

DELETE FROM ETL_ExecutionLog 
WHERE StartTime < DATEADD(DAY, -90, GETDATE());

-- 2. Purge old staging data
TRUNCATE TABLE STG_DimCustomer;
TRUNCATE TABLE STG_DimProduct;
TRUNCATE TABLE STG_DimSupplier;
TRUNCATE TABLE STG_DimCampaign;
TRUNCATE TABLE STG_DimPaymentMethod;
TRUNCATE TABLE STG_FactSales;
TRUNCATE TABLE STG_FactReturns;

-- 3. Validate SCD2 integrity
-- Ensure no overlapping date ranges
SELECT 
    Customer_ID,
    COUNT(*) AS ActiveVersions
FROM DimCustomer
WHERE Is_Current = 1
GROUP BY Customer_ID
HAVING COUNT(*) > 1; -- Should return 0 rows
```

### Alerting Rules

| Alert Type | Condition | Severity | Action |
|------------|-----------|----------|--------|
| **ETL Failure** | Package fails | Critical | Email + SMS to DBA |
| **Long Running ETL** | Duration > 90 min | Warning | Email to team |
| **High Error Rate** | Errors > 100 rows | Warning | Email to data team |
| **Row Count Variance** | > 10% difference | Critical | Email + investigate |
| **Disk Space Low** | < 10% free | Critical | Email to infrastructure |

### Audit & Compliance

**Data Lineage Tracking**:
- Record source system, load timestamp, batch ID for every row
- Maintain audit trail in `ETL_ExecutionLog`

**Change History**:
- SCD Type 2 automatically tracks all historical changes
- Store SCD change reason in `Change_Reason` column

**Retention Policy**:
- Keep 3 years of fact data in active warehouse
- Archive older data to separate database/blob storage
- Maintain dimension history indefinitely (SCD2)

---

## Appendix

### ETL Metrics & KPIs

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| Daily Load Success Rate | > 99% | 99.8% | ✓ |
| Average Load Duration | < 60 min | 45 min | ✓ |
| Data Quality Score | > 98% | 99.2% | ✓ |
| Lookup Failure Rate | < 0.1% | 0.05% | ✓ |

### Troubleshooting Guide

**Common Issues**:

1. **Timeout connecting to source**
   - Check network connectivity
   - Verify source database is online
   - Review firewall rules

2. **Lookup failures in FactSales**
   - Validate dimension load completed successfully
   - Check for missing dimension records
   - Review `ETL_ErrorLog` for specific IDs

3. **Slow dimension loads**
   - Check index fragmentation
   - Review SCD2 logic efficiency
   - Consider partitioning for large dimensions

4. **Duplicate records in fact tables**
   - Verify incremental load logic (LastLoadDate)
   - Check control table values
   - Review source data for duplicates

### Contact Information

**Data Warehouse Team**:
- DW Lead: [Name] - [email]
- ETL Developer: [Name] - [email]
- DBA: [Name] - [email]

**Escalation**:
- Level 1: DW Team (15 min response)
- Level 2: IT Manager (1 hour response)
- Level 3: CIO (same day response)

---

**Document Version**: 2.0  
**Last Updated**: February 4, 2026  
**Next Review**: May 4, 2026
