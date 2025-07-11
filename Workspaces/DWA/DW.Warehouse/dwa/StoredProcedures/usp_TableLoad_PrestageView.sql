/*
Description:Materialize VIEWs into _staging table. This is performance tweak if the view is expensive.
Example:	exec dwa.[usp_TableLoad_PrestageView] 'aw_int.Organization',null
History:	14/08/2023 Deepak, Created		
*/
CREATE   PROC [dwa].[usp_TableLoad_PrestageView] @SourceObject sysname,  @TargetObject [sysname] AS
BEGIN
	SET NOCOUNT ON
	DECLARE @sql nvarchar(4000)	=''
	IF @TargetObject is null 
		set @TargetObject = @SourceObject+ '_Prestage'
	SET @sql = 'DROP TABLE IF EXISTS ' + @TargetObject + ';' + CHAR(13)
	SET @sql = @sql + 'CREATE TABLE ' + @TargetObject + ' AS ' + CHAR(13)
	SET @sql = @sql + 'SELECT * FROM '  + @SourceObject + ';'
	PRINT '/* Prestage View. PrestageSourceFlag=1 */'
	PRINT @sql
	exec (@sql)

END