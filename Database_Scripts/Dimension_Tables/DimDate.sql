-- =============================================
-- Create DimDate Dimension Table
-- Pre-populated date dimension for 10+ years
-- =============================================

IF OBJECT_ID('DimDate', 'U') IS NOT NULL
    DROP TABLE DimDate;

CREATE TABLE DimDate (
    -- Primary Key (Integer format: YYYYMMDD)
    Date_Key INT NOT NULL,
    
    -- Full Date
    Full_Date DATE NOT NULL,
    
    -- Day Attributes
    Day TINYINT NOT NULL,                    -- 1-31
    Day_Name NVARCHAR(10) NOT NULL,          -- Monday, Tuesday, etc.
    Day_Of_Week TINYINT NOT NULL,            -- 1-7 (1=Monday, 7=Sunday)
    Day_Of_Year SMALLINT NOT NULL,           -- 1-366
    
    -- Week Attributes
    Week TINYINT NOT NULL,                   -- 1-53
    Week_Of_Year TINYINT NOT NULL,           -- 1-53
    
    -- Month Attributes
    Month TINYINT NOT NULL,                  -- 1-12
    Month_Name NVARCHAR(10) NOT NULL,        -- January, February, etc.
    Month_Name_Short NCHAR(3) NOT NULL,      -- Jan, Feb, etc.
    
    -- Quarter Attributes
    Quarter TINYINT NOT NULL,                -- 1-4
    Quarter_Name NVARCHAR(10) NOT NULL,      -- Q1, Q2, Q3, Q4
    
    -- Year Attributes
    Year SMALLINT NOT NULL,                  -- 2020, 2021, etc.
    Year_Month NCHAR(7) NOT NULL,            -- 2025-01
    
    -- Fiscal Period (Assuming fiscal year = calendar year; adjust if needed)
    Fiscal_Year SMALLINT NOT NULL,
    Fiscal_Quarter TINYINT NOT NULL,
    Fiscal_Month TINYINT NOT NULL,
    
    -- Flags
    Is_Weekend BIT NOT NULL,                 -- 1 = Yes, 0 = No
    Is_Holiday BIT NOT NULL DEFAULT 0,       -- 1 = Yes, 0 = No
    Is_Weekday BIT NOT NULL,                 -- 1 = Yes, 0 = No
    
    -- Holiday Name (if applicable)
    Holiday_Name NVARCHAR(50) NULL,
    
    -- Constraints
    CONSTRAINT PK_DimDate PRIMARY KEY CLUSTERED (Date_Key)
);

-- =============================================
-- Create Indexes for Performance
-- =============================================

-- Index on Full_Date for date range queries
CREATE NONCLUSTERED INDEX IX_DimDate_FullDate 
    ON DimDate(Full_Date);

-- Index on Year, Month for time-based grouping
CREATE NONCLUSTERED INDEX IX_DimDate_YearMonth 
    ON DimDate(Year, Month);

-- Index on Year, Quarter for quarterly analysis
CREATE NONCLUSTERED INDEX IX_DimDate_YearQuarter 
    ON DimDate(Year, Quarter);

-- Index on Is_Weekend for filtering
CREATE NONCLUSTERED INDEX IX_DimDate_IsWeekend 
    ON DimDate(Is_Weekend);

-- =============================================
-- Populate DimDate Table
-- Date range: 2016-01-01 to 2030-12-31 (adjust as needed)
-- =============================================

DECLARE @StartDate DATE = '2016-01-01';
DECLARE @EndDate DATE = '2030-12-31';
DECLARE @CurrentDate DATE = @StartDate;

WHILE @CurrentDate <= @EndDate
BEGIN
    INSERT INTO DimDate (
        Date_Key,
        Full_Date,
        Day,
        Day_Name,
        Day_Of_Week,
        Day_Of_Year,
        Week,
        Week_Of_Year,
        Month,
        Month_Name,
        Month_Name_Short,
        Quarter,
        Quarter_Name,
        Year,
        Year_Month,
        Fiscal_Year,
        Fiscal_Quarter,
        Fiscal_Month,
        Is_Weekend,
        Is_Holiday,
        Is_Weekday,
        Holiday_Name
    )
    VALUES (
        -- Date_Key: YYYYMMDD format
        CAST(FORMAT(@CurrentDate, 'yyyyMMdd') AS INT),
        
        -- Full_Date
        @CurrentDate,
        
        -- Day
        DAY(@CurrentDate),
        
        -- Day_Name
        DATENAME(WEEKDAY, @CurrentDate),
        
        -- Day_Of_Week (1=Monday, 7=Sunday)
        CASE 
            WHEN DATEPART(WEEKDAY, @CurrentDate) = 1 THEN 7  -- Sunday
            ELSE DATEPART(WEEKDAY, @CurrentDate) - 1
        END,
        
        -- Day_Of_Year
        DATEPART(DAYOFYEAR, @CurrentDate),
        
        -- Week
        DATEPART(WEEK, @CurrentDate),
        
        -- Week_Of_Year
        DATEPART(WEEK, @CurrentDate),
        
        -- Month
        MONTH(@CurrentDate),
        
        -- Month_Name
        DATENAME(MONTH, @CurrentDate),
        
        -- Month_Name_Short
        LEFT(DATENAME(MONTH, @CurrentDate), 3),
        
        -- Quarter
        DATEPART(QUARTER, @CurrentDate),
        
        -- Quarter_Name
        'Q' + CAST(DATEPART(QUARTER, @CurrentDate) AS NVARCHAR(1)),
        
        -- Year
        YEAR(@CurrentDate),
        
        -- Year_Month
        FORMAT(@CurrentDate, 'yyyy-MM'),
        
        -- Fiscal_Year (assuming fiscal year = calendar year)
        YEAR(@CurrentDate),
        
        -- Fiscal_Quarter
        DATEPART(QUARTER, @CurrentDate),
        
        -- Fiscal_Month
        MONTH(@CurrentDate),
        
        -- Is_Weekend
        CASE 
            WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 1  -- Sunday or Saturday
            ELSE 0
        END,
        
        -- Is_Holiday (default to 0, update separately)
        0,
        
        -- Is_Weekday
        CASE 
            WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 0  -- Sunday or Saturday
            ELSE 1
        END,
        
        -- Holiday_Name (default to NULL, update separately)
        NULL
    );
    
    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
END;

-- =============================================
-- Update Holidays (US Holidays)
-- =============================================

-- New Year's Day
UPDATE DimDate 
SET Is_Holiday = 1, Holiday_Name = 'New Year''s Day'
WHERE Month = 1 AND Day = 1;

-- Independence Day (US)
UPDATE DimDate 
SET Is_Holiday = 1, Holiday_Name = 'Independence Day'
WHERE Month = 7 AND Day = 4;

-- Christmas Day
UPDATE DimDate 
SET Is_Holiday = 1, Holiday_Name = 'Christmas Day'
WHERE Month = 12 AND Day = 25;

-- Thanksgiving (US - 4th Thursday of November)
UPDATE DimDate 
SET Is_Holiday = 1, Holiday_Name = 'Thanksgiving'
WHERE Month = 11 
  AND Day_Name = 'Thursday'
  AND Day BETWEEN 22 AND 28;



-- =============================================

-- =============================================
-- Verification Query
-- =============================================

-- Check record count and date range
SELECT 
    COUNT(*) AS Total_Records,
    MIN(Full_Date) AS Start_Date,
    MAX(Full_Date) AS End_Date,
    SUM(CASE WHEN Is_Weekend = 1 THEN 1 ELSE 0 END) AS Weekend_Days,
    SUM(CASE WHEN Is_Holiday = 1 THEN 1 ELSE 0 END) AS Holidays
FROM DimDate;

-- Sample records
SELECT TOP 10 * FROM DimDate ORDER BY Date_Key;
