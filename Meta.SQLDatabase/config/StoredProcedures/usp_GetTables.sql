/*
	Return Config Tables in Dependancy Order
	15-01-2025 Shruti, Added filter to return tables with config schema in recursive CTE
*/
CREATE PROCEDURE [config].[usp_GetTables]
AS
BEGIN
    SET NOCOUNT ON

	;WITH cte(lvl, object_id, name) AS (SELECT        1 AS Expr1, object_id, name
    FROM            sys.tables
    WHERE        (type_desc = 'USER_TABLE') AND (is_ms_shipped = 0) and name <> 'sysdiagrams' and SCHEMA_NAME(schema_id) = 'config'
    UNION ALL
    SELECT        cte_2.lvl + 1 AS Expr1, t.object_id, t.name
    FROM            cte AS cte_2 INNER JOIN
                            sys.tables AS t ON EXISTS
                                (SELECT        NULL AS Expr1
                                    FROM            sys.foreign_keys AS fk
                                    WHERE        (parent_object_id = t.object_id) AND (referenced_object_id = cte_2.object_id)) AND t.object_id <> cte_2.object_id AND cte_2.lvl < 30
    WHERE        (t.type_desc = 'USER_TABLE') AND (t.is_ms_shipped = 0) and SCHEMA_NAME(schema_id) = 'config')
    SELECT        TOP (100) PERCENT name, MAX(lvl) AS dependency_level
     FROM            cte AS cte_1
     GROUP BY name
     ORDER BY dependency_level, name

END

GO

