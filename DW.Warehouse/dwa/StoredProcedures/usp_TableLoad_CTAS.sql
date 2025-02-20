/*
Description:	CTAS Operation for Table Load framework - Execute main usp_TableLoad proc
Example: exec dwa.[usp_TableLoad] 'aw.FactFinance',NULL
		 exec dwa.[usp_TableLoad] NULL, 7 
History:	20/02/2025 Created		
*/
CREATE PROC [dwa].[usp_TableLoad_CTAS] @TableID [int],@TargetObject [sysname],@SourceObject [sysname],@SelectColumns [nvarchar](max),@JoinSQL [nvarchar](max),@SqlWhere [nvarchar](max),@PrestageJoinSQL [nvarchar](max),@DeltaJoinSql [nvarchar](max),@RelatedBusinessKeys [nvarchar](max) AS
BEGIN
	--SET XACT_ABORT ON 
	
	BEGIN 
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)	      
		  , @TablePrefix nvarchar(max)		  
		  , @PrestageTargetFlag bit
		  , @CTAS bit				/* 1 if CTAS statement required */
		  , @DeleteDDL nvarchar(4000)

	SELECT @TablePrefix= coalesce(t.TablePrefix, lower(left(replace(t.TableName, 'Dim',''),1)))
	, @PrestageTargetFlag = coalesce(t.PrestageTargetFlag,0)
	, @DeleteDDL=t.DeleteDDL
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID	
	
    IF @TargetObject IS NULL
	BEGIN
		RAISERROR ('No Rule found in edwTables for %s', 16, 1, @SourceObject)
	END

	SET @sql = 'CREATE TABLE ' + @TargetObject + CHAR(13) + 'AS' + CHAR(13)
	SET @sql = @sql + 'SELECT ' + @SelectColumns + CHAR(13)
	SET @sql = @sql + 'FROM ' + @SourceObject + ' ' + @TablePrefix

	IF coalesce(@RelatedBusinessKeys, '') > ''
	BEGIN
		SET @sql = @sql + coalesce(CHAR(13) + @JoinSQL, '')
	END

	IF @PrestageJoinSQL IS NOT NULL AND @PrestageTargetFlag = 1
		SET @sql = @sql + coalesce(CHAR(13) + @PrestageJoinSQL, '')

	IF @DeltaJoinSql IS NOT NULL
		SET @sql = @sql + coalesce(CHAR(13) + @DeltaJoinSQL, '')

	IF @PrestageTargetFlag = 0 AND @DeleteDDL IS NOT NULL
		SET @SqlWhere = coalesce(@SqlWhere + CHAR(13) + CHAR(9) + ' AND ', 'WHERE ') + 'NOT (' + @TablePrefix + '.' + @DeleteDDL + ')'

	IF @sqlWhere IS NOT NULL
		SET @sql = @sql + CHAR(13) + @sqlWhere;

	SET @sql=@sql + ';' + char(13)
	PRINT @sql
	EXEC (@sql)	

	END
END