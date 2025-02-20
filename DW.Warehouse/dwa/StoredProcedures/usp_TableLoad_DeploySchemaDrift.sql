/*
Description:	DeploySchemaDrift for Table Load framework - Execute main usp_TableLoad proc
Example:  EXEC dwa.[usp_TableLoad_DeploySchemaDrift] 4
History:	20/02/2025 Created		
*/
CREATE PROC [dwa].[usp_TableLoad_DeploySchemaDrift] @TableID [int] AS
BEGIN
	--SET XACT_ABORT ON 	
	BEGIN 
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)	      	  
		  , @SourceObject varchar(128)
		  , @TargetObject varchar(512)
		  , @TempTable varchar(102)
		  , @SelectColumns [nvarchar](max)
		  , @AddColumns [nvarchar](max)
		  , @RowChecksum [nvarchar](max)

	SELECT @SourceObject =t.SourceObject 
	, @TargetObject = t.SchemaName + '.' +  t.TableName
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID		
   
	SET @TempTable = @TargetObject+'_tmp'
	
		BEGIN
			IF OBJECT_ID(@TempTable) IS NOT NULL
				EXEC [dwa].[usp_TableDrop] @Table= @TempTable
			SET @SQL = 'CREATE TABLE ' +@TargetObject+'_tmp AS' + char(13) + 'SELECT * FROM '+@TargetObject +';'
			PRINT @SQL
			EXEC(@SQL)
			EXEC [dwa].[usp_TableDrop] @Table= @TargetObject;
			EXEC [dwa].[usp_TableCreate] @TableID = @TableID		

			SET  @AddColumns= (	
				SELECT string_agg(
						CASE WHEN c.is_nullable = 1 THEN 'NULL' 
							 WHEN c.name LIKE '%RowChecksum%' THEN '-1' 
							 WHEN st.name LIKE '%char%' AND c.is_nullable = 0 THEN '''''' 
							 WHEN st.name IN ('decimal', 'numeric', 'int') AND c.is_nullable = 0 THEN '0' 
						END + '  AS ' + c.name + ' ', ',')
				FROM sys.columns c
				INNER JOIN sys.types st ON c.user_type_id = st.user_type_id
				WHERE c.object_id = object_id(@TargetObject) AND c.name NOT IN (SELECT name FROM sys.columns WHERE object_id = object_id(@TempTable))
			)		

			SET @RowChecksum = (	
				SELECT string_agg(c.name, ',')
				FROM sys.columns c
				WHERE c.object_id = object_id(@TargetObject) 
				AND c.name IN (SELECT name FROM sys.columns WHERE object_id = object_id(@TempTable))
				AND c.name LIKE '%RowChecksum%')	

			SET @SelectColumns = (	
				SELECT string_agg(c.name, ',')
				FROM sys.columns c
				WHERE c.object_id = object_id(@TargetObject) AND c.name IN (SELECT name FROM sys.columns WHERE object_id = object_id(@TempTable))
				AND c.name NOT LIKE '%RowChecksum%')	
							
			SET @SQL = 'INSERT INTO ' + @TargetObject + char(13) + 'SELECT ' + @SelectColumns 
			IF @AddColumns  IS NOT NULL SET @SQL = @SQL + ' , ' +  @AddColumns 			
			IF @RowChecksum IS NOT NULL  SET @SQL = @SQL + ' , ' + '-1 AS' + @RowChecksum
			SET @SQL = @SQL + char(13) + 'FROM ' + @TempTable

			PRINT @SQL
			EXEC (@SQL)
			IF OBJECT_ID(@TempTable) IS NOT NULL
				EXEC [dwa].[usp_TableDrop] @Table=@TempTable
			END
	END
END