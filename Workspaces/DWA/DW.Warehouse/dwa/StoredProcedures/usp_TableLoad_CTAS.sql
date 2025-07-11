/*
Description:	CTAS Operation for Table Load framework - Execute main usp_TableLoad proc
Example: exec dwa.[usp_TableLoad] 'aw.FactFinance',NULL
		 exec dwa.[usp_TableLoad] NULL, 7 
History:	25/08/2022 Deepak, Created	
			17/03/2025 Bob, Tuning
*/
CREATE   PROC [dwa].[usp_TableLoad_CTAS] @TableID [int],@TargetObject [sysname],@SourceObject [sysname],@SelectColumns [nvarchar](max),@JoinSQL [nvarchar](max),@SqlWhere [nvarchar](max),@PrestageJoinSQL [nvarchar](max),@DeltaJoinSql [nvarchar](max),@RelatedBusinessKeys [nvarchar](max), @SqlWhereOuter varchar(4000), @InsertColumns varchar(4000), @TablePrefix nvarchar(128), @PrestageTargetFlag bit,@DeleteDDL varchar(4000),@RowChecksum bit  AS
BEGIN
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)	  
	
	SET @sql = 'DROP TABLE IF EXISTS ' + @TargetObject + ';' + CHAR(13)
	SET @sql = @Sql + 'CREATE TABLE ' + @TargetObject + CHAR(13) + 'AS' + CHAR(13)
	if len(@SqlWhereOuter) > 0 SET @sql =@sql +  'SELECT ' + @InsertColumns + ' FROM ' + CHAR(13) + '(' + CHAR(13) 
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

	IF LEN(@SqlWhereOuter ) > 0 SET @sql = @sql + CHAR(13) + ') f' + CHAR(13) + 'WHERE ' + @SqlWhereOuter

	SET @sql=@sql + ';' + char(13)
	PRINT @sql
	EXEC (@sql)	

END