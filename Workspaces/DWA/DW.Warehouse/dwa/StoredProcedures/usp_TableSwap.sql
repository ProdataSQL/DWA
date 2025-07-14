/*
Description:	Swap 2 table. useful for Green-Blue style deployments
				Assumes a schema called "tmp" exists
Example:		exec [dwa].[usp_TableSwap] 'aw_int.DimOrganization','aw.DimOrganization'
History:		03/08/2025 Bob, Created		
*/
CREATE     PROC [dwa].[usp_TableSwap] @SourceTable sysname, @TargetTable sysname
AS
BEGIN
	SET NOCOUNT ON
	DECLARE  @sql nvarchar(max)	=''
			,@SourceSchema sysname
			,@TargetSchema sysname
			,@TempTable sysname

	SELECT  @SourceSchema = coalesce(PARSENAME (@SourceTable,2),'dbo')
			,@TargetSchema = coalesce(PARSENAME (@TargetTable,2),'dbo')
			,@TempTable = 'tmp' + '.' + PARSENAME (@TargetTable,1)

	SET @sql ='/* dwa.usp_TableSwap ''' + @SourceTable + ''',''' + @TargetTable + ''' */' + CHAR(13)
	SET @sql =@sql + 'BEGIN TRANSACTION;' + CHAR(13)
	SET @sql = @sql + 'BEGIN TRY'+ CHAR(13)
	IF OBJECT_ID(@SourceTable) is not null
	BEGIN
		SET @sql = @sql +CHAR(9) + 'DROP TABLE IF EXISTS ' + @TempTable + ';' + CHAR(13)
		SET @sql=@sql + CHAR(9) +'ALTER SCHEMA tmp TRANSFER ' + @TargetTable + ';' + CHAR(13)
	END
	ELSE
		IF OBJECT_ID(@TempTable) is null
			return

	SET @sql=@sql + CHAR(9) +'ALTER SCHEMA ' + @TargetSchema  + ' TRANSFER ' + @SourceTable + ';' + CHAR(13)
	SET @sql = @sql +CHAR(9) + 'DROP TABLE IF EXISTS ' + @TempTable + ';' + CHAR(13)
	SET @sql = @sql + CHAR(9) +'COMMIT TRANSACTION;'+ CHAR(13)
	SET @sql = @sql + 'END TRY'+ CHAR(13)
	SET @sql = @sql + 'BEGIN CATCH'+ CHAR(13)
	SET @sql = @sql + CHAR(9) +'ROLLBACK TRANSACTION;'+ CHAR(13)
	SET @sql = @sql + CHAR(9) +'THROW'+ CHAR(13)
	SET @sql = @sql + 'END CATCH;'+ CHAR(13)
	print @sql
	exec (@sql)

END