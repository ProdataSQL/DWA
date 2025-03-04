/*
Description:	INSERT Operation for Table Load framework - Execute main usp_TableLoad proc
Example: exec dwa.[usp_TableLoad] 'aw.FactFinance',NULL,NULL
		 exec dwa.[usp_TableLoad] NULL, 1,NULL
History:		20/02/2025  Created	
*/
CREATE   PROC [dwa].[usp_TableLoad_Insert] @TableID [int],@TargetObject [sysname],@SourceObject [sysname],@InsertColumns [nvarchar](max),@SelectColumns [nvarchar](max),@JoinSQL [nvarchar](max),@SqlWhere [nvarchar](max),@WhereJoinSQL [nvarchar](max),@Exists [bit] AS
BEGIN
	--SET XACT_ABORT ON 
	
	BEGIN 
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)	      
		  , @TablePrefix nvarchar(max)
		  , @BusinessKeys nvarchar(max)
		  , @PrestageTargetFlag bit
		  , @PrestageTargetObject sysname /* Full Name of Final Target Table if prestaging */

	IF @TableID IS NULL
		SELECT @TableID = t.TableID FROM Meta.config.edwTables t WHERE ((t.SchemaName + '.' +  t.TableName =@TargetObject) OR t.TableName =@TargetObject)
	SET @sqlWhere = COALESCE(@sqlWhere,'')
	SELECT @TablePrefix= coalesce(t.TablePrefix, lower(left(replace(t.TableName, 'Dim',''),1)))
	, @BusinessKeys =t.BusinessKeys
	, @PrestageTargetFlag = coalesce(t.PrestageTargetFlag,0)
	, @PrestageTargetObject =coalesce(t.PrestageSchema,'stg') + '.'+ coalesce(t.PrestageTable, t.TableName)
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID	
	
    IF @TargetObject IS NULL
	BEGIN
		RAISERROR ('No Rule found in edwTables for %s', 16, 1, @SourceObject)
	END
	IF @PrestageTargetFlag = 1
		BEGIN
			SELECT @SelectColumns =  (SELECT string_agg( c.name, ',') FROM sys.columns c WHERE c.object_id = object_id(@TargetObject) AND  c.name NOT IN('RowVersionNo', 'RowOperation' ) )
			SELECT @InsertColumns=   @SelectColumns
			SET @sqlWhere =  'WHERE RowOperation=''I'''
		END

	SET @SQL ='INSERT INTO ' + @TargetObject + ' (' + @InsertColumns + ')'	+ char(13)
	IF @BusinessKeys is not null and @Exists =1	SET @SQL =@SQL + 'SELECT '+ @TablePrefix +'.* FROM (' + char(13) 
	SET @SQL =@SQL + 'SELECT ' + @SelectColumns + char(13) + 'FROM ' + @SourceObject + ' '+  @TablePrefix   
	SET @SQL=  coalesce(@SQL +  char(13)+  @JoinSQL ,@SQL)	

	IF @BusinessKeys is not null and @Exists =1	SET @SQL =@SQL + char(13) + @sqlWhere +  ' ) ' + @TablePrefix 
	SET @JoinSQL =null
	IF @BusinessKeys is not null and @Exists =1
	BEGIN		
	    
		SELECT @JoinSQL = coalesce( NULL+  ' AND ','WHERE ' ) + 'NOT EXISTS (SELECT * FROM ' + @TargetObject + ' t WHERE '
		IF @WhereJoinSQL IS NOT NULL
			SELECT @JoinSQL = @JoinSQL + (SELECT string_agg(rtrim(bk.value), ' AND ') + ')'  FROM string_split(rtrim(@WhereJoinSQl), char(13)) bk)
		ELSE
			SELECT @JoinSQL = @JoinSQL + (
							SELECT string_agg(@TablePrefix + '.' + ltrim(bk.value) + '=t.' + ltrim(bk.value), ' AND ') + ')'
							FROM string_split(@BusinessKeys, ',') bk
							)  
		IF @JoinSQL is not null SET @sql=  @sql + char(13) + @JoinSQL
	END
	IF @JoinSQL IS NULL AND  @sqlWhere IS NOT NULL SET @sql=@sql + char(13) +  @sqlWhere         
	SET @SQL=@SQL + ';' +char(13) 
	PRINT @sql 
		EXEC (@sql)	
	END
END