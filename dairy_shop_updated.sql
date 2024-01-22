SELECT * FROM DAIRY_DATASET

--MAKING A COPY OF THE DATASET TO WORK

SELECT *INTO DAIRY_COPY FROM DAIRY_DATASET

SELECT * FROM DAIRY_COPY

---GETTING THE META DATA

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'DAIRY_COPY';

--ALTERING THE COLUMN NAME

EXEC sp_rename 'DAIRY_COPY.Total Land Area (acres)', 'TOTAL_AREA';

EXEC sp_rename 'DAIRY_COPY.APPROX_TOTAL_REVENUE', 'TOTAL_REVENUE';

EXEC sp_rename 'DAIRY_COPY.[Quantity (liters/kg)]', 'QUANTITY';

EXEC sp_rename 'DAIRY_COPY.[Shelf Life (days)]', 'SHELF_LIFE_IN_DAYS';

--CHECKING For DUPLICATE RECORDS

WITH CTE AS (
SELECT LOCATION,TOTAL_AREA,[NUMBER OF COWS],[FARM SIZE] ,
       ROW_NUMBER()OVER( PARTITION BY LOCATION,TOTAL_AREA,[NUMBER OF COWS],[FARM SIZE] ORDER BY LOCATION ASC ) AS  RN 
	   FROM DAIRY_COPY
	   )

SELECT * FROM CTE WHERE RN>1

--UPDATING THE 'TOTAL_VALUES' COLUMN TO DISPLAY TWO DECIMAL POINTS.

ALTER TABLE DAIRY_COPY
ADD TOTAL_VALUES FLOAT

UPDATE DAIRY_COPY SET TOTAL_VALUES=CAST([TOTAL VALUE] AS DECIMAL(10,2))

--DROPPING THE COLUMN 

ALTER TABLE DAIRY_COPY
DROP COLUMN [TOTAL VALUE]



--CHANGING THE DATA TYPE OF COLUMNS  FROM DATETIME TO DATE FOR PRODUCTION DATE AND EXPIRATION DATE

ALTER TABLE DAIRY_COPY
ALTER COLUMN [PRODUCTION DATE] DATE

ALTER TABLE DAIRY_COPY
ALTER COLUMN [EXPIRATION DATE] DATE

ALTER TABLE DAIRY_COPY
ALTER COLUMN [DATE] DATE

-- ADDING A COLUMN BASED ON THE COUNT OF COWS
-- 'GROUP3' HAS THE HIGHEST NUMBER OF COWS, SAY 45
-- 'GROUP2' HAS 30 COWS
-- 'GROUP1' HAS 15 COWS

ALTER TABLE DAIRY_COPY
ADD NO_COWS_BUCKET VARCHAR(50)

UPDATE DAIRY_COPY SET NO_COWS_BUCKET=CASE WHEN [NUMBER OF COWS] <=25 THEN 'GROUP 1'
                                           WHEN [NUMBER OF COWS] >25 AND [NUMBER OF COWS]<=55 THEN 'GROUP 2'
			                               ELSE 'GROUP3' END 


--1. Create a stored procedure to get total revenue over time

--ANS: It appears that the total revenue was high in the year 2019
CREATE PROCEDURE GetYearlyTotalRevenue
AS
BEGIN
    SELECT
        YEAR(DATE) AS YEARS,
        SUM(TOTAL_REVENUE) AS TOTAL_REVENUE
        FROM DAIRY_COPY
        GROUP BY YEAR(DATE)
        ORDER BY YEAR(DATE) ASC;
END;

-- Executing the stored procedure
EXEC GetYearlyTotalRevenue;



--2. Top 5 products and brands with high revenue and quantity sold

-- Top 5 products with the highest revenue and quantity sold

SELECT TOP 5  [PRODUCT NAME],SUM(TOTAL_REVENUE) AS REVENUE ,SUM( [Quantity Sold (liters/kg)])AS QUANTITY_SOLD FROM DAIRY_COPY 
GROUP BY [PRODUCT NAME]
ORDER BY REVENUE DESC

-- Top 5 Brands with the highest revenue and quantity sold

SELECT TOP 5  [BRAND],SUM(TOTAL_REVENUE) AS REVENUE ,SUM( [Quantity Sold (liters/kg)])AS QUANTITY_SOLD FROM DAIRY_COPY 
GROUP BY [BRAND]
ORDER BY REVENUE DESC




--3. Find the top and bottom 3 locations yielding the maximum and minimum revenue


WITH CTE AS (
            SELECT [LOCATION] ,
                   SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,
            	   DENSE_RANK()OVER(ORDER BY SUM(TOTAL_REVENUE) DESC) AS RN 
                   FROM DAIRY_COPY
            	   GROUP BY [LOCATION]
) SELECT * FROM CTE WHERE RN<=3 OR RN>(SELECT RN FROM CTE WHERE RN IN (SELECT MAX(RN)-3 FROM CTE))



--4.Analyzing the correlation between total quantity and farm size  

-- Quantity analysis for products with units in liters

SELECT  [FARM SIZE] ,
        CAST(SUM(QUANTITY) AS DECIMAL(10,0)) AS QUANTITY_IN_STOCK, 
		CAST(SUM([TOTAL_VALUES]) AS DECIMAL(10,0))AS MARKET_PRICE 
FROM DAIRY_COPY
WHERE [PRODUCT NAME] IN ('MILK','BUTTERMILK','LASSI')
GROUP BY [FARM SIZE]
ORDER BY  SUM(QUANTITY)  DESC

-- Quantity analysis for products with units in KILOGRAMS

SELECT  [FARM SIZE] ,
        CAST(SUM(QUANTITY) AS DECIMAL(10,0)) AS QUANTITY_IN_STOCK, 
		CAST(SUM([TOTAL_VALUES]) AS DECIMAL(10,0))AS MARKET_PRICE  
FROM DAIRY_COPY
WHERE [PRODUCT NAME]  NOT IN ('MILK','BUTTERMILK','LASSI')
GROUP BY [FARM SIZE]
ORDER BY  SUM(QUANTITY)  DESC

-- ANS: Farm size does not have much impact on the quantity 

--5.Analyzing the correlation between total quantity and the number of cows 

-- Quantity analysis for products with units in liters

SELECT  NO_COWS_BUCKET ,
        CAST(SUM(QUANTITY) AS DECIMAL(10,0)) AS QUANTITY_IN_STOCK, 
		CAST(SUM([TOTAL_VALUES]) AS DECIMAL(10,0))AS MARKRT_PRICE  
FROM DAIRY_COPY
WHERE [PRODUCT NAME] IN ('MILK','BUTTERMILK','LASSI')
GROUP BY [NO_COWS_BUCKET]
ORDER BY  SUM(QUANTITY)  DESC


-- Quantity analysis for products with units in kilograms

SELECT  NO_COWS_BUCKET , 
        CAST(SUM(QUANTITY) AS DECIMAL(10,0)) AS QUANTITY_IN_STOCK, 
		CAST(SUM([TOTAL_VALUES]) AS DECIMAL(10,0))AS MARKRT_PRICE  
FROM DAIRY_COPY
WHERE [PRODUCT NAME]  NOT IN ('MILK','BUTTERMILK','LASSI')
GROUP BY [NO_COWS_BUCKET]
ORDER BY  SUM(QUANTITY)  DESC

---- ANS:Group 3 has the maximum quantity of stock available, hence No_of_cows count  have  impact on the quantity 

--6.Get the top 5 products that generate the highest revenue in each location

CREATE PROCEDURE SP_TOP5_PRODUCTS(@LOCATION AS NVARCHAR (50))
AS 
BEGIN

   SELECT LOCATION,[PRODUCT NAME],REVENUE FROM (
     SELECT * ,
	    DENSE_RANK()OVER(PARTITION BY LOCATION ORDER BY REVENUE DESC) AS RN 
	 FROM (
         SELECT [LOCATION],[PRODUCT NAME],SUM(TOTAL_REVENUE) AS REVENUE FROM DAIRY_COPY 
         GROUP BY [LOCATION],[PRODUCT NAME]
           ) AS Q  
    ) AS W WHERE RN <=5 AND LOCATION=@LOCATION

END

EXEC SP_TOP5_PRODUCTS @LOCATION='DELHI'


--7. HIGH REVENUE YIELDING PRODUCTS FOR EACH BRAND

ALTER PROCEDURE SP_TOP_REVENUEPROD_EACHBRAND(@BRAND AS NVARCHAR(50))
AS 
BEGIN

     SELECT BRAND,[PRODUCT NAME],REVENUE FROM (
          SELECT *,DENSE_RANK()OVER(PARTITION BY BRAND ORDER BY REVENUE DESC) AS RN 
		  FROM (
              SELECT   [BRAND],[PRODUCT NAME],SUM(TOTAL_REVENUE) AS REVENUE   FROM DAIRY_COPY 
              GROUP BY [BRAND],[PRODUCT NAME]
              --ORDER BY BRAND ASC ,REVENUE DESC
          )AS Q
     ) AS W WHERE RN <=3 AND BRAND=@BRAND

END


---FAMILIAR PRODUCTS IN EACH BRAND

ALTER PROCEDURE SP_TOP_SOLDPROD_EACHBRAND(@BRAND AS NVARCHAR(50))
AS 
BEGIN

     SELECT BRAND,[PRODUCT NAME],SOLD FROM (
         SELECT *,DENSE_RANK()OVER(PARTITION BY BRAND ORDER BY SOLD DESC) AS RN 
		 FROM (
             SELECT   [BRAND],[PRODUCT NAME],SUM([Quantity Sold (liters/kg)]) AS SOLD  FROM DAIRY_COPY 
             GROUP BY [BRAND],[PRODUCT NAME]
             --ORDER BY BRAND ASC ,REVENUE DESC
         )AS Q
     ) AS W WHERE RN <=3 AND BRAND=@BRAND

END

EXEC SP_TOP_REVENUEPROD_EACHBRAND @BRAND='AMUL'

EXEC SP_TOP_SOLDPROD_EACHBRAND @BRAND='AMUL'


---8.GETTING FAMILAR PRODUCT OF THE BRAND LOCATION WISE

ALTER PROCEDURE SP_EACH_BRAND_TOP_PROD_EACH_LOC(@BRAND AS NVARCHAR (50))
AS 
BEGIN


WITH CTE AS (
    SELECT [BRAND],LOCATION,[PRODUCT NAME],REVENUE ,
	       DENSE_RANK()OVER(PARTITION BY LOCATION ORDER BY REVENUE DESC) AS RN FROM (
          SELECT 
		      BRAND,
			  LOCATION,
			  [PRODUCT NAME],
			  SUM(TOTAL_REVENUE) AS REVENUE

		  FROM DAIRY_COPY 
          GROUP BY BRAND,LOCATION,[Product Name]
		  
         
      ) AS W WHERE W.BRAND='AMUL'
)SELECT * FROM CTE WHERE RN=1

 END

EXEC SP_EACH_BRAND_TOP_PROD_EACH_LOC @BRAND='AMUL'

--9.COMPETITORS EACH YEAR

CREATE PROCEDURE SP_EACH_YEAR_COMPE (@YEAR AS INT )
AS 
BEGIN 
    SELECT * FROM (
      SELECT *,DENSE_RANK()OVER(ORDER BY REVENUE DESC) AS RN FROM (
         SELECT YEAR(DATE) AS YEARS,BRAND,SUM(TOTAL_REVENUE) AS REVENUE  FROM DAIRY_COPY WHERE YEAR(DATE)=@YEAR
         GROUP BY YEAR(DATE), [BRAND]
         --ORDER BY YEARS,BRAND
      ) AS Q 
    ) AS W WHERE RN<=3

END

EXEC SP_EACH_YEAR_COMPE @YEAR=2022

--10.Sales based on mode of delivery
--Ans:It appears that the most sales are  generated through retail

SELECT [SALES CHANNEL],SUM([Quantity Sold (liters/kg)]) AS SOLD FROM DAIRY_COPY 
GROUP BY [SALES CHANNEL]
ORDER BY  SOLD DESC


--INVENTORY MANAGEMENT

--11.Examine the quantity in stock, minimum stock threshold, and reorder quantity to optimize inventory levels.
--Identifying products that frequently go out of stock 

SELECT LOCATION,DATE,BRAND,
    [PRODUCT NAME],
    [Quantity in Stock (liters/kg)] AS QuantityInStock,
    [Minimum Stock Threshold (liters/kg)] AS MinimumStockThreshold,
    [Reorder Quantity (liters/kg)] AS ReorderQuantity
FROM DAIRY_COPY
WHERE [Quantity in Stock (liters/kg)] < [Minimum Stock Threshold (liters/kg)]

--Identifying products that have excess stock


SELECT LOCATION,DATE,BRAND,
    [PRODUCT NAME],
    [Quantity in Stock (liters/kg)] AS QuantityInStock,
    [Minimum Stock Threshold (liters/kg)] AS MinimumStockThreshold,
    [Reorder Quantity (liters/kg)] AS ReorderQuantity
FROM DAIRY_COPY
WHERE [Quantity in Stock (liters/kg)] > [Reorder Quantity (liters/kg)];

---12.Identifying products that are expired

SELECT
    [PRODUCT NAME],
    [SHELF_LIFE_IN_DAYS] AS ShelfLife,
    [Storage Condition] AS StorageConditions,
    [Production Date],
    [Expiration Date]
FROM DAIRY_COPY
WHERE [Expiration Date] < GETDATE()  
ORDER BY[Expiration Date] ASC;
 

 --13.Identifying products that have generated profit

 select * from DAIRY_COPY

SELECT [Product Name], [Price per Unit],[Price per Unit (sold)] FROM DAIRY_COPY
WHERE [Price per Unit (sold)]>([Price per Unit])




