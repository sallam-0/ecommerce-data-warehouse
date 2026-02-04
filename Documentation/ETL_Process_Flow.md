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
---

## ETL Architecture

### High-Level Architecture Diagram

```
┌───────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEM (OLTP)                       │
│  ┌──────────┐  ┌─────────┐  ┌─────────┐  ┌──────────┐             │
│  │ customer │  │ product │  │ orders  │  │ supplier │             │
│  │          │  │         │  │         │  │          │             │
│  └──────────┘  └─────────┘  └─────────┘  └──────────┘             │
│       │             │             │             │                 │
└───────┼─────────────┼─────────────┼─────────────┼─────────────────┘
        │             │             │             │
        │    EXTRACT (SSIS OLE DB Source)        │
        │             │             │             │
        ▼             ▼             ▼             ▼
┌──────────────────────────────────────────────────────────────────┐
│                    STAGING AREA (Data Warehouse)                 │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐    │
│  │  STG_DimCustomer │  │  STG_DimProduct  │  │STG_DimSupplier│   │
│  └──────────────────┘  └──────────────────┘  └──────────────┘    │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐    │
│  │ STG_DimCampaign  │  │STG_DimPayment    │  │STG_FactSales │    │
│  │                  │  │   Method         │  │              │    │
│  └──────────────────┘  └──────────────────┘  └──────────────┘    │
│  ┌──────────────────┐                                            │
│  │STG_FactReturns   │                                            │
│  │                  │                                            │ 
│  └──────────────────┘                                            │
└───────┬──────────────────┬──────────────────────┬────────────────┘
        │                  │                      │
        │    TRANSFORM (Data Cleansing, Lookups, SCD)
        │                  │                      │
        ▼                  ▼                      ▼
┌───────────────────────────────────────────────────────────────────┐
│              DATA WAREHOUSE (Star Schema)                         │
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │                    DIMENSION TABLES                        │   │
│  │  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐    │   │
│  │  │ DimCustomer │  │  DimProduct  │  │   DimSupplier   │    │   │
│  │  │   (SCD2)    │  │    (SCD2)    │  │     (SCD1)      │    │   │
│  │  └─────────────┘  └──────────────┘  └─────────────────┘    │   │
│  │  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐    │   │
│  │  │   DimDate   │  │ DimCampaign  │  │DimPaymentMethod │    │   │
│  │  │(Pre-loaded) │  │    (SCD1)    │  │     (SCD1)      │    │   │
│  │  └─────────────┘  └──────────────┘  └─────────────────┘    │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │                      FACT TABLES                           │   │
│  │  ┌──────────────┐  ┌──────────────┐                        │   │
│  │  │  FactSales   │  │ FactReturns  │                        │   │
│  │  │              │  │              │                        │   │
│  │  └──────────────┘  └──────────────┘                        │   │
│  └────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────┘
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
| `customer` | Customer master data | customer_id, first_name, last_name, email | 500 |
| `product` | Product catalog | product_id, name, price, subcategory_id | 500 |
| `subcategory` | Product subcategories | subcategory_id, subcategory_name, category_id | 100 |
| `category` | Product categories | category_id, category_name | 50 |
| `supplier` | Supplier information | supplier_id, supplier_name, email | 20 |
| `orders` | Order headers | order_id, customer_id, order_date, campaign_id | 50,000 |
| `orderitem` | Order line items | orderitem_id, order_id, product_id, quantity | 275,000 |
| `returns` | Product returns | return_id, order_id, product_id, return_date | 2,500 |
| `payment_method` | Payment methods | payment_method_id, payment_method | 6 |
| `marketing_campaigns` | Marketing campaigns | campaign_id, campaign_name, offer_week | 17 |


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
│                                                             │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 3: Basic Transformations                               │
│         - Data type conversions                             │
│         - NULL handling                                     │
│         - Data cleansing (TRIM, UPPER, etc.)                │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 4: Load to Staging                                     │
│         OLE DB Destination → STG_ tables                    │
│         Fast Load option enabled                            │
└─────────────────────────────────────────────────────────────┘
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
```

### DimDate - Pre-Population

**Load Type**: One-time full load  
**SCD Type**: N/A (static reference data)



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
│      SELECT c.customer_id, c.first_name, c.last_name,       │
│             c.email, c.phone, c.date_of_birth, c.gender,    │
│             c.city, c.state, c.country, c.postal_code,      │
│             c.registration_date, c.customer_segment,        │
│             c.modified_date                                 │
│      FROM customer c                                        │
│                                                             │
│    Destination: STG_DimCustomer                             │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Data Cleansing in Staging                                │
│    - Derived Column: TRIM(First_Name), TRIM(Last_Name)      │
│    - Derived Column: UPPER(Email)                           │
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
│    New Records → INSERT with Is_Current = 1                 │
│    Changed Records → Update old (Is_Current = 0),           │
│                      Insert new (Is_Current = 1)            │
└─────────────────────────────────────────────────────────────┘
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
│      SELECT p.product_id, p.name, p.description,            │
│             sc.subcategory_name, c.category_name,           │
│             s.supplier_id, s.supplier_name,                 │
│             p.unit_price, p.unit_cost, p.size, p.color,     │
│             p.is_active, p.modified_date                    │
│      FROM product p                                         │
│      JOIN subcategory sc ON p.subcategory_id = sc.id        │
│      JOIN category c ON sc.category_id = c.category_id      │
│      LEFT JOIN supplier s ON p.supplier_id = s.supplier_id  │
│                                                             │
│    Destination: STG_DimProduct                              │
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
│    Destination: STG_DimSupplier                             │
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
│                                                             │
│    Destination: STG_DimCampaign                             │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Calculate Campaign Duration in Staging                   │
│    Derived Column: DATEDIFF(day, Start_Date, End_Date)      │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Merge from Staging to Dimension                          │
└─────────────────────────────────────────────────────────────┘
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
│      SELECT oi.orderitem_id, oi.order_id,                   │
│             o.customer_id, oi.product_id,                   │
│             o.campaign_id, o.payment_method_id,             │
│             o.order_date, o.ship_date,                      │
│             oi.quantity, oi.unit_price, oi.unit_cost,       │
│             oi.discount_amount, o.tax_amount,               │
│             o.shipping_cost                                 │
│      FROM orderitem oi                                      │
│      INNER JOIN orders o ON oi.order_id = o.order_id        │
│      WHERE o.order_date >= @LastLoadDate                    │
│    Destination: STG_FactSales                               │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Calculate Derived Measures in Staging                    │
│    - Sales_Amount = Quantity * Unit_Price                   │
│    - Cost_Amount = Quantity * Unit_Cost                     │
│    - Gross_Profit = Sales_Amount - Cost_Amount              │
│    - Net_Sales = Sales_Amount - Discount_Amount             │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Dimension Key Lookups from Staging                       │
│    Lookup Transformations:                                  │
│      Customer_Key ← DimCustomer (Customer_ID, Is_Current=1) │
│      Product_Key ← DimProduct (Product_ID, Is_Current=1)    │
│      Campaign_Key ← DimCampaign (Campaign_ID)               │
│      PaymentMethod_Key ← DimPaymentMethod (Payment_ID)      │
│      Order_Date_Key ← DimDate (Order_Date)                  │
│      Ship_Date_Key ← DimDate (Ship_Date)                    │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Error Handling                                           │
│    Conditional Split:                                       │
│      - Valid Lookups → Continue to FactSales                │
│      - Failed Lookups → Error Output (Log & Alert)          │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. Load to FactSales                                        │
│    OLE DB Destination (Fast Load)                           │
└─────────────────────────────────────────────────────────────┘
```





### FactReturns - Transaction Fact Table

**Load Type**: Incremental  
**Granularity**: One row per return transaction  


**SSIS Package Flow**:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Truncate STG_FactReturns                                 │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Extract to Staging                                       │
│    Source Query:                                            │
│      SELECT r.return_id, r.order_id, r.orderitem_id,        │
│             r.product_id, r.customer_id, r.return_date,     │
│             r.return_quantity, r.return_amount,             │
│             r.return_reason, r.refund_amount,               │
│             r.restocking_fee                                │
│      FROM returns r                                         │
│      WHERE r.return_date >= @LastLoadDate                   │
│    Destination: STG_FactReturns                             │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Dimension Key Lookups from Staging                       │
│    Lookups:                                                 │
│      Customer_Key ← DimCustomer                             │
│      Product_Key ← DimProduct                               │
│      Return_Date_Key ← DimDate                              │
│      Order_Key ← FactSales (Order_ID + OrderItem_ID)        │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Calculate Return Metrics in Staging                      │
│    - Net_Refund_Amount = Refund_Amount - Restocking_Fee     │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Load to FactReturns                                      │
└─────────────────────────────────────────────────────────────┘
```


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

```

**Connection Managers**:

```
SourceOLTP    → SQL Server connection to transactional DB
TargetDW      → SQL Server connection to data warehouse
```




---

