/*
Description:	DELETE Operation for Table Load framework - Execute main usp_TableLoad proc	
Example: exec dwa.[usp_TableLoad] 'aw.FactFinance',NULL
		 exec dwa.[usp_TableLoad] NULL, 7 
History:	20/02/2025  Created		
*/
CREATE   PROC [dwa].[usp_TableLoad_Delete] @TableID int,@SelectColumns nvarchar(max),@JoinSQL nvarchar(max) AS
BEGIN
	--SET XACT_ABORT ON 	
	BEGIN 
	SET NOCOUNT ON
	DECLARE @SQL nvarchar(max)
	      , @SourceObject sysname
		  , @TargetObject [sysname]
		  , @TablePrefix nvarchar(max)
		  , @BusinessKeys nvarchar(max)
		  , @DeleteDDL nvarchar(4000)

	SELECT @TargetObject = t.SchemaName + '.' + t.TableName, @TablePrefix= coalesce(t.TablePrefix, lower(left(replace(t.TableName, 'Dim',''),1)))
	, @SourceObject =t.SourceObject
	, @BusinessKeys =t.BusinessKeys
	, @DeleteDDL=t.DeleteDDL
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID	
	
    IF @TargetObject IS NULL
	BEGIN
		RAISERROR ('No Rule found in edwTables for %s', 16, 1, @SourceObject)
	END

	SET @SQL ='DELETE t ' + char(13) + 'FROM ' +  @TargetObject  + ' t' + char(13) 
	IF @JoinSQL IS NOT NULL
		BEGIN
			SET @SQL =@SQL + 'INNER JOIN  ( SELECT ' + @SelectColumns + char(13) + 'FROM ' + @SourceObject +' ' + @TablePrefix 
			SET @SQL=  coalesce(@SQL +  char(13)+  @JoinSQL ,@SQL)
			SET @SQL = @SQL + char(13)+' WHERE '+@TablePrefix +'.'+ @DeleteDDL + char(13)+ ') s ON '
			SET @SQL = @SQL + (	SELECT string_agg ('s.' + ltrim(bk.value) + '=t.' + ltrim(bk.value)  , ' AND ') 
								FROM string_split(@BusinessKeys,',') bk ) 
		END
	ELSE
		BEGIN
			SET @sql = @sql + 'INNER JOIN ' + @SourceObject + ' s ON '
			SET @sql = @sql + (	SELECT string_agg ('s.' + ltrim(bk.value) + '=t.' + ltrim(bk.value)  , ' AND ') 
								FROM string_split(@BusinessKeys,',') bk ) + char(13) + 'AND s.' + @DeleteDDL
		END	 
	PRINT @SQL + char(13)
	EXEC (@SQL)

	END
END