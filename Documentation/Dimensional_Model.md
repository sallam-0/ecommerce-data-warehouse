# Dimensional Model Documentation

## Overview
This document describes the star schema design for the Retail Analytics Data Warehouse.

## Business Process
The data warehouse supports three main business processes:
1. **Sales Analysis**: Track and analyze product sales transactions
2. **Returns Management**: Monitor and analyze product returns

## Grain Statements

### FactSales
- **Grain**: One row per order line item
- **Granularity**: Individual product purchase within an order

### FactReturns
- **Grain**: One row per product return
- **Granularity**: Individual product return transaction


## Dimension Tables

### DimDate
- **Type**: Conformed dimension
- **SCD Type**: Static (pre-populated)
- **Range**: 2016-2030
- **Key Attributes**: Year, Quarter, Month, Week, Day

### DimCustomer
- **Type**: Conformed dimension
- **SCD Type**: Type 2 (track history)
- **Key Attributes**: Customer name, email, country
- **Slowly Changing Attributes**: Name, Email, country

### DimProduct
- **Type**: Conformed dimension
- **SCD Type**: Type 2 (track history)
- **Key Attributes**: Product name, description, price, category
- **Slowly Changing Attributes**: Price, description,category

### DimSupplier
- **Type**: Standard dimension
- **SCD Type**: Type 1 (overwrite)
- **Key Attributes**: Supplier name, email

### DimPaymentMethod
- **Type**: Standard dimension
- **SCD Type**: Type 1 (overwrite)
- **Key Attributes**: Payment method name

### DimCampaign
- **Type**: Standard dimension
- **SCD Type**: Type 1 (overwrite)
- **Key Attributes**: Campaign name, offer week

## Fact Tables

### FactSales
**Measures:**
- Quantity (Additive)
- Subtotal (Additive)
- Discount (Additive)
- LineTotal (Additive, Derived)

**Dimensions:**
- Date (Order Date)
- Customer
- Product
- Supplier
- Payment Method
- Campaign (nullable)

### FactReturns
**Measures:**
- Amount Refunded (Additive)
- Quantity Returned (Additive)

**Dimensions:**
- Date (Return Date)
- Customer
- Product
- Payment Method


## Relationships
```
DimDate ----< FactSales
DimCustomer ----< FactSales
DimProduct ----< FactSales
DimSupplier ----< FactSales
DimPaymentMethod ----< FactSales
DimCampaign ----<? FactSales (optional)

DimDate ----< FactReturns
DimCustomer ----< FactReturns
DimProduct ----< FactReturns
DimPaymentMethod ----< FactReturns

```

## Key Business Rules

1. **Sales**: 
   - Each order can have multiple line items
   - Not all sales have associated campaigns
   - Discount cannot exceed subtotal

2. **Returns**:
   - Returns must reference an existing order
   - Refund amount should not exceed original sale amount
   - One return per product per order (business rule)

## Unknown/Default Members

All dimensions include a record with key = -1 for unknown/missing references:
- Customer_Key = -1: Unknown Customer
- Product_Key = -1: Unknown Product
- Campaign_Key = -1: No Campaign

## Data Refresh Strategy

- **Dimensions**: SCD Type 2 dimensions maintain history
- **Facts**: Append-only (no updates to fact records)
- **Load Frequency**: Daily incremental loads
- **Historical Data**: Retained indefinitely
