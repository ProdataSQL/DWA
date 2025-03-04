/*	Drop a Star Schema Table
	Example:[dwa].[usp_TableDrop] 'aw.DimDate'
	History:
			20/02/2025 Created
*/
CREATE PROCEDURE [dwa].[usp_TableDrop] @Table [sysname]
AS
BEGIN
	DECLARE @object_id INT
	DECLARE @sql NVARCHAR(4000)
	DECLARE @is_external BIT

	SELECT @object_id = object_id, @is_external = is_external
	FROM sys.tables
	WHERE object_id = object_id(@Table)

	IF @object_id IS NOT NULL
	BEGIN
		SET @sql = 'DROP ' + CASE WHEN @is_external = 1 THEN 'EXTERNAL ' ELSE '' END + 'TABLE ' + @Table + ';'
		PRINT @sql
		EXEC (@sql)
	END
END