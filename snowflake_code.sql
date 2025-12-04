-- Use correct db & schema
USE DATABASE RECRUITMENT_DB;
USE SCHEMA CANDIDATE_00355;

-- Check the file exists
LIST @RECRUITMENT_DB.PUBLIC.S3_FOLDER;

-- Create a temporary file format for exploration
CREATE OR REPLACE FILE FORMAT temp_pipe_format
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1 -- 0 for seeing the headers
    ENCODING = 'UTF8'; 
    --column count 31

-- Inspect the returning data from the file
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
       $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
       $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31
FROM @RECRUITMENT_DB.PUBLIC.S3_FOLDER/flights.gz
(FILE_FORMAT => temp_pipe_format)
LIMIT 11;

-- Detect column types from the file
SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@RECRUITMENT_DB.PUBLIC.S3_FOLDER/flights.gz',
        FILE_FORMAT => 'temp_pipe_format'
    )
);

-- Create file format (not temp)
CREATE OR REPLACE FILE FORMAT pipe_format
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ENCODING = 'UTF8';

-- Create the table using the 
CREATE OR REPLACE TABLE "STG_FLIGHTS_RAW" (
    "TRANSACTIONID" NUMBER(9,0),
    "FLIGHTDATE" NUMBER(8,0),
    "AIRLINECODE" TEXT,
    "AIRLINENAME" TEXT,
    "TAILNUM" TEXT,
    "FLIGHTNUM" NUMBER(4,0),
    "ORIGINAIRPORTCODE" TEXT,
    "ORIGAIRPORTNAME" TEXT,
    "ORIGINCITYNAME" TEXT,
    "ORIGINSTATE" TEXT,
    "ORIGINSTATENAME" TEXT,
    "DESTAIRPORTCODE" TEXT,
    "DESTAIRPORTNAME" TEXT,
    "DESTCITYNAME" TEXT,
    "DESTSTATE" TEXT,
    "DESTSTATENAME" TEXT,
    "CRSDEPTIME" NUMBER(4,0),
    "DEPTIME" NUMBER(4,0),
    "DEPDELAY" NUMBER(4,0),
    "TAXIOUT" NUMBER(4,0),
    "WHEELSOFF" NUMBER(4,0),
    "WHEELSON" NUMBER(4,0),
    "TAXIIN" NUMBER(4,0),
    "CRSARRTIME" NUMBER(4,0),
    "ARRTIME" NUMBER(4,0),
    "ARRDELAY" NUMBER(4,0),
    "CRSELAPSEDTIME" NUMBER(3,0),
    "ACTUALELAPSEDTIME" NUMBER(3,0),
    "CANCELLED" BOOLEAN,
    "DIVERTED" BOOLEAN,
    "DISTANCE" TEXT
);

-- Copy the raw data to the table
COPY INTO "STG_FLIGHTS_RAW"
FROM @RECRUITMENT_DB.PUBLIC.S3_FOLDER/flights.gz
FILE_FORMAT = pipe_format
ON_ERROR = 'CONTINUE';

-- Check for errors and the row counts
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'STG_FLIGHTS_RAW',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
)); -- ERROR_COUNT = 0

SELECT COUNT(*)
FROM @RECRUITMENT_DB.PUBLIC.S3_FOLDER/flights.gz
(FILE_FORMAT => pipe_format); -- 1191805

SELECT COUNT(*) AS loaded_rows FROM "STG_FLIGHTS_RAW"; -- 1191805

-- Check for duplicates
SELECT "TRANSACTIONID", COUNT(*)
FROM "STG_FLIGHTS_RAW"
GROUP BY "TRANSACTIONID"
HAVING COUNT(*) > 1;

-- Check for NULL or empty critical fields
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN "TRANSACTIONID" IS NULL THEN 1 ELSE 0 END) AS missing_transactionid, -- 0
    SUM(CASE WHEN "FLIGHTDATE" IS NULL THEN 1 ELSE 0 END) AS missing_flightdate, -- 0
    SUM(CASE WHEN "DISTANCE" IS NULL OR "DISTANCE" = '' THEN 1 ELSE 0 END) AS missing_distance -- 0
FROM "STG_FLIGHTS_RAW";

-- Check date formats (format: YYYYMMDD like 20020101)
SELECT DISTINCT "FLIGHTDATE"
FROM "STG_FLIGHTS_RAW"
LIMIT 100;

-- Check time formats (should be HHMM)
SELECT DISTINCT "DEPTIME", "ARRTIME", "CRSDEPTIME", "CRSARRTIME"
FROM "STG_FLIGHTS_RAW"
LIMIT 100;

-- Check for negative or impossible values
SELECT *
FROM "STG_FLIGHTS_RAW"
WHERE TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) < 0
   OR "DEPDELAY" < -150
   OR "ARRDELAY" < -150;

-- Examine airline and airport names for cleanup needs
SELECT DISTINCT "AIRLINENAME"
FROM "STG_FLIGHTS_RAW"
ORDER BY "AIRLINENAME";

-- **America West Airlines Inc.: HP (Merged with US Airways 9/05.Stopped reporting 10/07.)
-- **US Airways Inc.: US (Merged with America West 9/05. Reporting for both starting 10/07.)
-- **Comair Inc.: OH (1)
-- **ExpressJet Airlines Inc. (1): XE & ExpressJet Airlines Inc.: EV

-- Check for text/number format issues in CANCELLED column
SELECT DISTINCT "CANCELLED", COUNT(*)
FROM "STG_FLIGHTS_RAW"
GROUP BY "CANCELLED";

-- Create DIM_AIRLINE table
CREATE OR REPLACE TABLE "DIM_AIRLINE" (
    "AIRLINE" VARCHAR(10) PRIMARY KEY,
    "AIRLINENAME" VARCHAR(100),
    "NOTES" VARCHAR(500)  -- Historical context (mergers, acquisitions, etc.)
);

-- Load with cleaned airline names and extracted notes
INSERT INTO "DIM_AIRLINE" ("AIRLINE", "AIRLINENAME", "NOTES")
SELECT DISTINCT
    "AIRLINECODE" AS "AIRLINE",
    -- Clean airline name: remove ": CODE" pattern and parenthetical notes
    -- Step 1: Remove everything from colon onward (captures ": HP", ": OH", ": US", etc.)
    -- Step 2: Remove parenthetical content like "(merged...)"
    -- Step 3: Trim whitespace
    TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE("AIRLINENAME", ':\\s*[A-Z0-9]+.*$', ''),
        '\\s*\\([^)]*\\)\\s*',
        ''
    )) AS "AIRLINENAME",
    -- Extract notes from parentheses for historical context
    REGEXP_SUBSTR("AIRLINENAME", '\\(([^)]*)\\)', 1, 1, 'e', 1) AS "NOTES"
FROM "STG_FLIGHTS_RAW"
WHERE "AIRLINECODE" IS NOT NULL 
  AND "AIRLINENAME" IS NOT NULL;

-- Create the DIM_AIRPORT table
CREATE OR REPLACE TABLE "DIM_AIRPORT" (
    "AIRPORT" VARCHAR(10) PRIMARY KEY,
    "AIRPORTNAME" VARCHAR(200),
    "CITY" VARCHAR(100),
    "STATE" VARCHAR(50)
);

-- Load origin airports
INSERT INTO "DIM_AIRPORT" ("AIRPORT", "AIRPORTNAME", "CITY", "STATE")
SELECT DISTINCT
    "ORIGINAIRPORTCODE" AS "AIRPORT",
    -- Remove concatenated city/state (e.g., "AlbuquerqueNM: Albuquerque International Sunport" -> "Albuquerque International Sunport")
    TRIM(REGEXP_REPLACE("ORIGAIRPORTNAME", '^[^:]+:\\s*', '')) AS "AIRPORTNAME",
    "ORIGINCITYNAME" AS "CITY",
    "ORIGINSTATENAME" AS "STATE"
FROM "STG_FLIGHTS_RAW"
WHERE "ORIGINAIRPORTCODE" IS NOT NULL;

-- Load destination airports (merge to avoid duplicates)
MERGE INTO "DIM_AIRPORT" tgt
USING (
    SELECT DISTINCT
        "DESTAIRPORTCODE" AS "AIRPORT",
        TRIM(REGEXP_REPLACE("DESTAIRPORTNAME", '^[^:]+:\\s*', '')) AS "AIRPORTNAME",
        "DESTCITYNAME" AS "CITY",
        "DESTSTATENAME" AS "STATE"
    FROM "STG_FLIGHTS_RAW"
    WHERE "DESTAIRPORTCODE" IS NOT NULL
) src
ON tgt."AIRPORT" = src."AIRPORT"
WHEN NOT MATCHED THEN
    INSERT ("AIRPORT", "AIRPORTNAME", "CITY", "STATE")
    VALUES (src."AIRPORT", src."AIRPORTNAME", src."CITY", src."STATE");

-- Create DIM_DATE table
CREATE OR REPLACE TABLE "DIM_DATE" (
    "DATE" DATE PRIMARY KEY,
    "YEAR" NUMBER(4,0),
    "QUARTER" NUMBER(1,0),
    "MONTH" NUMBER(2,0),
    "MONTHNAME" VARCHAR(20),
    "DAYOFMONTH" NUMBER(2,0),
    "DAYOFWEEK" NUMBER(1,0),
    "DAYNAME" VARCHAR(20),
    "WEEKOFYEAR" NUMBER(2,0),
    "ISWEEKEND" NUMBER(1,0)
);

-- First, find the min and max dates in the data
-- Then generate a continuous date range for BI tools
INSERT INTO "DIM_DATE"
WITH date_range AS (
    SELECT 
        MIN(TRY_TO_DATE(TO_VARCHAR("FLIGHTDATE"), 'YYYYMMDD')) AS min_date,
        MAX(TRY_TO_DATE(TO_VARCHAR("FLIGHTDATE"), 'YYYYMMDD')) AS max_date
    FROM "STG_FLIGHTS_RAW"
    WHERE TRY_TO_DATE(TO_VARCHAR("FLIGHTDATE"), 'YYYYMMDD') IS NOT NULL
),
date_series AS (
    SELECT 
        DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, dr.min_date) AS generated_date,
        dr.max_date
    FROM date_range dr,
         TABLE(GENERATOR(ROWCOUNT => 10000))
    QUALIFY generated_date <= max_date
)
SELECT 
    generated_date AS "DATE",
    YEAR(generated_date) AS "YEAR",
    QUARTER(generated_date) AS "QUARTER",
    MONTH(generated_date) AS "MONTH",
    MONTHNAME(generated_date) AS "MONTHNAME",
    DAYOFMONTH(generated_date) AS "DAYOFMONTH",
    DAYOFWEEK(generated_date) AS "DAYOFWEEK",
    DAYNAME(generated_date) AS "DAYNAME",
    WEEKOFYEAR(generated_date) AS "WEEKOFYEAR",
    CASE 
        WHEN DAYOFWEEK(generated_date) IN (0, 6) THEN 1
        ELSE 0
    END AS "ISWEEKEND"
FROM date_series;

-- Create the FACT_FLIGHTS table
CREATE OR REPLACE TABLE "FACT_FLIGHTS" (
    "TRANSACTIONID" VARCHAR(50) PRIMARY KEY,
    "FLIGHTDATE" DATE,
    "AIRLINE" VARCHAR(10),
    "ORIGAIRPORT" VARCHAR(10),
    "DESTAIRPORT" VARCHAR(10),
    "DEPTIME" TIME,
    "DEPDELAY" NUMBER(10,2),
    "ARRTIME" TIME,
    "ARRDELAY" NUMBER(10,2),
    "CANCELLED" NUMBER(1,0),
    "DISTANCE" NUMBER(10,2),
    "DISTANCEGROUP" VARCHAR(20),
    "DEPDELAYGT15" NUMBER(1,0),
    "NEXTDAYARR" NUMBER(1,0),
    FOREIGN KEY ("AIRLINE") REFERENCES "DIM_AIRLINE"("AIRLINE"),
    FOREIGN KEY ("ORIGAIRPORT") REFERENCES "DIM_AIRPORT"("AIRPORT"),
    FOREIGN KEY ("DESTAIRPORT") REFERENCES "DIM_AIRPORT"("AIRPORT")
);

INSERT INTO "FACT_FLIGHTS"
SELECT 
    "TRANSACTIONID",
    
    -- Convert date number to DATE type (format: YYYYMMDD)
    -- First convert NUMBER to TEXT, then parse as date
    TRY_TO_DATE(TO_VARCHAR("FLIGHTDATE"), 'YYYYMMDD') AS "FLIGHTDATE",
    
    "AIRLINECODE" AS "AIRLINE",
    "ORIGINAIRPORTCODE" AS "ORIGAIRPORT",
    "DESTAIRPORTCODE" AS "DESTAIRPORT",
    
    -- Convert time strings (HHMM format) to TIME type
    -- Handle times like "0830" or "1945"
    TRY_TO_TIME(
        LPAD("DEPTIME", 4, '0') || ':00', 
        'HH24MI:SS'
    ) AS "DEPTIME",
    
    CAST("DEPDELAY" AS NUMBER(10,2)) AS "DEPDELAY",
    
    TRY_TO_TIME(
        LPAD("ARRTIME", 4, '0') || ':00', 
        'HH24MI:SS'
    ) AS "ARRTIME",
    
    CAST("ARRDELAY" AS NUMBER(10,2)) AS "ARRDELAY",
    -- CANCELLED is BOOLEAN type (TRUE/FALSE), convert to binary
    CASE 
        WHEN "CANCELLED" = TRUE THEN 1
        ELSE 0
    END AS "CANCELLED",
    -- DISTANCE may have 'miles' text, extract numeric part
    TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER(10,2)) AS "DISTANCE",
    
    -- CALCULATED COLUMN 1: DISTANCEGROUP
    -- Bins distance into 100-mile increments: "0-100 miles", "201-300 miles", etc.
    -- Use the cleaned numeric value from REGEXP_REPLACE
    CASE 
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) IS NULL THEN NULL
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 100 THEN '0-100 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 200 THEN '101-200 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 300 THEN '201-300 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 400 THEN '301-400 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 500 THEN '401-500 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 600 THEN '501-600 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 700 THEN '601-700 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 800 THEN '701-800 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 900 THEN '801-900 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 1000 THEN '901-1000 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 1100 THEN '1001-1100 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 1200 THEN '1101-1200 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 1300 THEN '1201-1300 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 1400 THEN '1301-1400 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 1500 THEN '1401-1500 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 1600 THEN '1501-1600 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 1700 THEN '1601-1700 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 1800 THEN '1701-1800 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 1900 THEN '1801-1900 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 2000 THEN '1901-2000 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 2100 THEN '2001-2100 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 2200 THEN '2101-2200 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 2300 THEN '2201-2300 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 2400 THEN '2301-2400 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 2500 THEN '2401-2500 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 2600 THEN '2501-2600 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 2700 THEN '2601-2700 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 2800 THEN '2701-2800 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 2900 THEN '2801-2900 miles'
        WHEN TRY_CAST(REGEXP_REPLACE("DISTANCE", '[^0-9.]', '') AS NUMBER) <= 3000 THEN '2901-3000 miles'
        ELSE '3000+ miles'
    END AS "DISTANCEGROUP",
    
    -- CALCULATED COLUMN 2: DEPDELAYGT15
    -- Indicates if departure delay > 15 minutes (1 = yes, 0 = no)
    CASE 
        WHEN "DEPDELAY" > 15 THEN 1
        ELSE 0
    END AS "DEPDELAYGT15",
    
    -- CALCULATED COLUMN 3: NEXTDAYARR
    -- Indicates if arrival time is next day after departure (1 = yes, 0 = no)
    -- Logic: If ARRTIME < DEPTIME, it wrapped to next day
    CASE 
        WHEN TRY_TO_TIME(LPAD("ARRTIME", 4, '0') || ':00', 'HH24MI:SS') < 
             TRY_TO_TIME(LPAD("DEPTIME", 4, '0') || ':00', 'HH24MI:SS') 
        THEN 1
        ELSE 0
    END AS "NEXTDAYARR"
    
FROM "STG_FLIGHTS_RAW"
WHERE "TRANSACTIONID" IS NOT NULL;

-- Create the view VW_FLIGHTS 
CREATE OR REPLACE VIEW "VW_FLIGHTS" AS
SELECT 
    -- Primary identifier
    f."TRANSACTIONID",
    
    -- Date/time information
    f."FLIGHTDATE",
    f."DEPTIME",
    f."ARRTIME",
    
    -- Date dimension attributes
    dt."YEAR",
    dt."QUARTER",
    dt."MONTH",
    dt."MONTHNAME",
    dt."DAYOFMONTH",
    dt."DAYOFWEEK",
    dt."DAYNAME",
    dt."WEEKOFYEAR",
    dt."ISWEEKEND",
    
    -- Airline information (from dimension)
    a."AIRLINE",
    a."AIRLINENAME",
    
    -- Origin airport information (from dimension)
    o."AIRPORT" AS "ORIGAIRPORT",
    o."AIRPORTNAME" AS "ORIGAIRPORTNAME",
    o."CITY" AS "ORIGCITY",
    o."STATE" AS "ORIGSTATE",
    
    -- Destination airport information (from dimension)
    d."AIRPORT" AS "DESTAIRPORT",
    d."AIRPORTNAME" AS "DESTAIRPORTNAME",
    d."CITY" AS "DESTCITY",
    d."STATE" AS "DESTSTATE",
    
    -- Flight metrics
    f."DEPDELAY",
    f."ARRDELAY",
    f."CANCELLED",
    f."DISTANCE",
    
    -- Calculated columns (required)
    f."DISTANCEGROUP",
    f."DEPDELAYGT15",
    f."NEXTDAYARR"
    
FROM "FACT_FLIGHTS" f
INNER JOIN "DIM_AIRLINE" a ON f."AIRLINE" = a."AIRLINE"
INNER JOIN "DIM_AIRPORT" o ON f."ORIGAIRPORT" = o."AIRPORT"
INNER JOIN "DIM_AIRPORT" d ON f."DESTAIRPORT" = d."AIRPORT"
INNER JOIN "DIM_DATE" dt ON f."FLIGHTDATE" = dt."DATE";

-- Create a materialised view for better performance (not available as a feature)
/* CREATE MATERIALIZED VIEW "MV_FLIGHTS" AS
SELECT * FROM "VW_FLIGHTS"; */

-- **Validation
-- 1. Row count check (should match staging table, accounting for referential integrity)
SELECT COUNT(*) AS staging_count FROM "STG_FLIGHTS_RAW"; -- 1191805
SELECT COUNT(*) AS fact_count FROM "FACT_FLIGHTS"; -- 1191805
SELECT COUNT(*) AS view_count FROM "VW_FLIGHTS"; -- 1191805
/* SELECT COUNT(*) AS mv_count FROM "MV_FLIGHTS";  -- Should match VW_FLIGHTS */ -- Not available as a feature in snowflake

-- 2. Check calculated columns
SELECT 
    "DISTANCE",
    "DISTANCEGROUP",
    "DEPDELAY",
    "DEPDELAYGT15",
    "DEPTIME",
    "ARRTIME",
    "NEXTDAYARR"
FROM "VW_FLIGHTS"
LIMIT 100;

-- 3. Verify DISTANCEGROUP bins
SELECT "DISTANCEGROUP", COUNT(*), AVG("DISTANCE") AS avg_distance
FROM "VW_FLIGHTS"
GROUP BY "DISTANCEGROUP"
ORDER BY avg_distance;

-- 4. Verify DEPDELAYGT15 logic
SELECT 
    SUM(CASE WHEN "DEPDELAY" > 15 THEN 1 ELSE 0 END) AS should_be_flagged, -- 168013
    SUM("DEPDELAYGT15") AS actually_flagged -- 168013
FROM "VW_FLIGHTS"; 

-- 5. Verify NEXTDAYARR logic (check red-eye flights)
SELECT *
FROM "VW_FLIGHTS"
WHERE "NEXTDAYARR" = 1
LIMIT 20;

-- 6. Check dimension table cleanliness
SELECT "AIRLINENAME" FROM "DIM_AIRLINE" ORDER BY "AIRLINENAME";
SELECT "AIRPORTNAME" FROM "DIM_AIRPORT" LIMIT 50;

-- 7. Check for required columns in view
DESCRIBE VIEW "VW_FLIGHTS";

-- 8. Test the view in BI-like aggregations
SELECT 
    "AIRLINENAME",
    COUNT(*) AS total_flights,
    AVG("DEPDELAY") AS avg_departure_delay,
    SUM("DEPDELAYGT15") AS delayed_flights,
    SUM("CANCELLED") AS cancelled_flights,
    AVG("DISTANCE") AS avg_distance
FROM "VW_FLIGHTS"
GROUP BY "AIRLINENAME"
ORDER BY total_flights DESC;

SELECT COUNT(*) FROM "VW_FLIGHTS";