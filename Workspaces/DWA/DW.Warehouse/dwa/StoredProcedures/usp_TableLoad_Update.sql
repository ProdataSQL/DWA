/*
Description:	UPDATE Operation for Table Load framework - Execute main usp_TableLoad proc
Example: exec dwa.[usp_TableLoad] 'aw.FactFinance',NULL
		 exec dwa.[usp_TableLoad] NULL, 1 ,NULL,NULL
History:	20/02/2025 Created		
*/
CREATE PROC [dwa].[usp_TableLoad_Update] @TableID [int],@TargetObject [sysname],@SourceObject [sysname],@UpdateColumns [nvarchar](max),@SelectColumns [nvarchar](max),@JoinSQL [nvarchar](max),@SqlWhere [nvarchar](max),@WhereJoinSQL [nvarchar](max),@Exists [bit] AS
BEGIN
	--SET XACT_ABORT ON 
	BEGIN 
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)	      
		  , @TablePrefix nvarchar(max)
		  , @BusinessKeys nvarchar(max)
		  , @PrestageTargetFlag bit

	SELECT  @TablePrefix= coalesce(t.TablePrefix, lower(left(replace(t.TableName, 'Dim',''),1)))
	, @BusinessKeys =t.BusinessKeys
	, @PrestageTargetFlag = t.PrestageTargetFlag
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID	
	
    IF @TargetObject IS NULL
	BEGIN
		RAISERROR ('No Rule found in edwTables for %s', 16, 1, @SourceObject)
	END
	IF @PrestageTargetFlag = 1
		BEGIN
			SELECT @SelectColumns =  '*'
			SET @sqlWhere =  'WHERE RowOperation=''U'''
		END

	SET @JoinSQL = ' '+COALESCE(@JoinSQL,'')  + ' '+ COALESCE(@SqlWhere,'') + ') '+@TablePrefix+' ON ' + char(13) 
	SET @SQL = 'UPDATE t ' + char(13) + 'SET '
	SET @SQL = @SQL + @UpdateColumns + char(13)
	SET @SQL = @SQL + 'FROM ' + @TargetObject  + ' t' + char(13) 			

	IF @JoinSQL IS NOT NULL  
	BEGIN
		SET @SQL =@SQL + 'INNER JOIN (SELECT ' + @SelectColumns  + ' FROM ' + @SourceObject +' ' + @TablePrefix 
		SET @SqlWhere = NULL
	END						
    ELSE
		SET @SQL = @SQL + ' INNER JOIN ' + @SourceObject + ' ' + @TablePrefix + ' ON ' 

	SET @SQL=  coalesce(@SQL +  @JoinSQL ,@SQL)
	SET @SQL = @SQL + (	SELECT string_agg (@TablePrefix + '.' + ltrim(bk.value) + '=t.' + ltrim(bk.value)  , ' AND ') 	FROM string_split(@BusinessKeys,',') bk ) 	
	IF COALESCE(@PrestageTargetFlag,0) = 0 AND charindex ('.RowChecksum',@UpdateColumns) > 0 
		SET @SQL = @SQL + char(13) + 'AND ' + @TablePrefix + '.RowChecksum <> t.RowChecksum'  
	IF @SqlWhere is not null SET @sql=@sql + char(13) + @SqlWhere
	SET @sql =@sql + ';'
	PRINT @sql 
	EXEC (@sql)	
	END
END