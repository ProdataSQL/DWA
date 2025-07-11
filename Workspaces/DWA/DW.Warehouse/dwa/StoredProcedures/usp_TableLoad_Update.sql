/*
Description:	UPDATE Operation for Table Load framework - Execute main usp_TableLoad proc
Example: exec dwa.[usp_TableLoad] 'aw.FactFinance',NULL
		 exec dwa.[usp_TableLoad] NULL, 2 ,NULL,NULL
History:	03/08/2023 Deepak, Created		
*/
CREATE   PROC [dwa].[usp_TableLoad_Update] @TableID [int],@TargetObject [sysname],@SourceObject [sysname],@UpdateColumns [nvarchar](max),@SelectColumns [nvarchar](max),@JoinSQL [nvarchar](max),@SqlWhere [nvarchar](max),@WhereJoinSQL [nvarchar](max),@Exists [bit] , @TablePrefix sysname,@BusinessKeys varchar(4000), @PrestageTargetFlag bit, @SqlWhereOuter varchar(4000)  AS
BEGIN
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)	      

	--IF @PrestageTargetFlag = 1

	SET @JoinSQL = ' '+COALESCE(@JoinSQL,'')  + ' '+ COALESCE(@SqlWhere,'') + ') '+@TablePrefix+' ON ' + char(13) 
	SET @SQL = 'UPDATE t ' + char(13) + 'SET '
	SET @SQL = @SQL + @UpdateColumns + char(13)
	SET @SQL = @SQL + 'FROM ' + @TargetObject  + ' t' + char(13) 			

	print @JoinSQL
	IF @JoinSQL IS NOT NULL  and @PrestageTargetFlag =0
	BEGIN
		SET @SQL =@SQL + 'INNER JOIN (SELECT ' + @SelectColumns  + ' FROM ' + @SourceObject +' ' + @TablePrefix + CHAR(13)
		SET @SqlWhere = NULL
	END						
    ELSE
		SET @SQL = @SQL + 'INNER JOIN ' + @SourceObject + ' ' + @TablePrefix + CHAR(13) + 'ON ' 

	if @PrestageTargetFlag=0 SET @SQL=  coalesce(@SQL +  @JoinSQL ,@SQL)
	SET @SQL = @SQL + (	SELECT string_agg (@TablePrefix + '.' + ltrim(bk.value) + '=t.' + ltrim(bk.value)  , ' AND ') 	FROM string_split(@BusinessKeys,',') bk ) 	
	SET @SQL = @SQL + char(13) + 'AND ' + @TablePrefix + '.RowChecksum <> t.RowChecksum'  
	
	IF len(@SqlWhereOuter) > 0 
		SET @SqlWhere = coalesce (@SqlWhere + ' AND ', 'WHERE ' ) + @SqlWhereOuter  
	
	IF @SqlWhere is not null SET @sql=@sql + char(13) + @SqlWhere
	SET @sql =@sql + ';'
	PRINT @sql 
	EXEC (@sql)	
END