/*   Description: Create Numbers Tables
     History:    
		20/02/2025 Created                
 */
CREATE PROC [int].[usp_LoadNumbers] AS
BEGIN
    SET NOCOUNT ON;
  
    IF OBJECT_ID('int.Numbers') IS NOT NULL
    BEGIN
        DROP TABLE int.Numbers
    END

   BEGIN
        CREATE TABLE int.Numbers(
            [n] [bigint]  NULL
        )        
    END

   ;WITH  L0   AS (SELECT 1 AS n UNION ALL SELECT 1),  
    L1   AS (SELECT 1 AS n FROM L0 AS a CROSS JOIN L0 AS b),  
    L2   AS (SELECT 1 AS n FROM L1 AS a CROSS JOIN L1 AS b),  
    L3   AS (SELECT 1 AS n FROM L2 AS a CROSS JOIN L2 AS b),  
    L4   AS (SELECT 1 AS n FROM L3 AS a CROSS JOIN L3 AS b),  
    L5   AS (SELECT 1 AS n FROM L4 AS a CROSS JOIN L4 AS b),  
    Nums AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM L5)



   INSERT INTO int.Numbers(n)
    SELECT TOP(10958) n FROM Nums ORDER BY n;
END