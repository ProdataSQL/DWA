
/*
	Truncate ALL Config Tables. 
	Only run this for deployment or recovery
	15-01-2025 Shruti, Updated Query to return Schema Name within CTE  
*/
CREATE PROCEDURE [config].[usp_TruncateAll]
AS
BEGIN
    SET NOCOUNT ON
	DECLARE @sql nvarchar(max)

	-- DIASBLE Triggers
	DROP TABLE IF EXISTS #tr
	SELECT t.Name AS TriggerName, o.Name AS TableName 
	INTO #tr
	FROM sys.triggers t 
	INNER JOIN sys.objects o ON t.parent_id = o.object_id
	INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
	WHERE s.name = 'config'  AND t.type = 'TR'
	SELECT @sql = STRING_AGG ('DISABLE TRIGGER [' + TriggerName + '] ON [config].[' + Tablename + ']', ';')
	FROM #tr 
	print @sql

	exec (@sql)

	--DELETE Data from tables
	;WITH cte(lvl, object_id, name, SchemaName) AS (SELECT        1 AS Expr1, object_id, name, SCHEMA_NAME(schema_id) AS SchemaName
    FROM            sys.tables
    WHERE        (type_desc = 'USER_TABLE') AND (is_ms_shipped = 0) and name <> 'sysdiagrams' and SCHEMA_NAME(schema_id)='config'
    UNION ALL
    SELECT        cte_2.lvl + 1 AS Expr1, t.object_id, t.name, SCHEMA_NAME(schema_id) AS SchemaName
    FROM            cte AS cte_2 INNER JOIN
                            sys.tables AS t ON EXISTS
                                (SELECT        NULL AS Expr1
                                    FROM            sys.foreign_keys AS fk
                                    WHERE        (parent_object_id = t.object_id) AND (referenced_object_id = cte_2.object_id)) AND t.object_id <> cte_2.object_id AND cte_2.lvl < 30
    WHERE        (t.type_desc = 'USER_TABLE') AND (t.is_ms_shipped = 0) )
	, t as (
	    SELECT        TOP (100) PERCENT SchemaName, name, MAX(lvl) AS dependency_level
		FROM            cte AS cte_1
		GROUP BY SchemaName, name
		ORDER BY dependency_level, name
	 )
	 SELECT @sql = STRING_AGG  ('DELETE FROM ' + SchemaName + '.' + name,';')  WITHIN GROUP (ORDER BY dependency_level DESC, name DESC)
	FROM t
  	print @sql

	exec (@sql)

	--ENABLE Triggers after deletion is completed
	SELECT @sql = STRING_AGG ('ENABLE TRIGGER [' + TriggerName + '] ON [config].[' + Tablename + ']', ';')
	FROM #tr 
	print @sql

	exec (@sql)

END

GO

