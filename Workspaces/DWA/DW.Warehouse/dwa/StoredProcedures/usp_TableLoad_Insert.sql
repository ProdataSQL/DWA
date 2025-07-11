/*
Description:	INSERT Operation for Table Load framework - Execute main usp_TableLoad proc
Example: exec dwa.[usp_TableLoad] 'aw.FactFinance',NULL,NULL
		 exec dwa.[usp_TableLoad] NULL, 1,NULL
History:		03/08/2023 Deepak, Created	
*/
CREATE   PROC [dwa].[usp_TableLoad_Insert] @TableID [int],@TargetObject [sysname],@SourceObject [sysname],@InsertColumns [nvarchar](max),@SelectColumns [nvarchar](max),@JoinSQL [nvarchar](max),@SqlWhere [nvarchar](max),@WhereJoinSQL [nvarchar](max),@Exists [bit], @TablePrefix sysname,@BusinessKeys varchar(4000), @PrestageTargetFlag bit,@PrestageTargetObject sysname, @SqlWhereOuter varchar(4000)  AS
BEGIN
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)	      
	SET @sqlWhere = COALESCE(@sqlWhere,'')

	SET @SQL ='INSERT INTO ' + @TargetObject + ' (' + @InsertColumns + ')'	+ char(13)
	IF len(@SqlWhereOuter) > 0 or (@BusinessKeys is not null and @Exists =1)  SET @SQL =@SQL + 'SELECT ' + @InsertColumns + ' FROM (' + char(13)
	IF @PrestageTargetFlag=1 
		SET @SQL =@SQL + 'SELECT ' + @InsertColumns + char(13) + 'FROM ' + @SourceObject + ' '+  @TablePrefix   
	ELSE
		SET @SQL =@SQL + 'SELECT ' + @SelectColumns + char(13) + 'FROM ' + @SourceObject + ' '+  @TablePrefix   
	SET @SQL=  coalesce(@SQL +  char(13)+  @JoinSQL ,@SQL)	

	IF len(@SqlWhereOuter) > 0 or (@BusinessKeys is not null and @Exists =1) 	SET @SQL =@SQL + char(13) + @sqlWhere +  ' ) ' + @TablePrefix 
	SET @JoinSQL =null
	IF @BusinessKeys is not null and @Exists =1
	BEGIN		    
		SELECT @JoinSQL = coalesce('WHERE '  + @SqlWhereOuter + CHAR(13) + 'AND ' , 'WHERE ' ) + 'NOT EXISTS (SELECT * FROM ' + @TargetObject + ' t WHERE '
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