/*
    Return a List of Tables for CDC and their Primary Keys
    Used By: Extract-Parquet-CDC Notebook
    Test: 
        exec [config].[usp_cdcSqlTables] @LakehouseName='LH'
    History:    15/06/2025 Bob, Created           
                29/07/2025 Kristan, added SqlTables param

*/
CREATE PROCEDURE [config].[usp_cdcSqlTables]
(
    @LakehouseName sysname = 'FabricLH',
    @SqlTables sysname = 'config.cdcSqlTables'
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql nvarchar(max)

    SET @sql = 'SELECT [Table], PrimaryKeys
        FROM ' + @SqlTables + '
        WHERE LakehouseName = @LakehouseName AND Enabled = 1'

    EXEC sp_executesql @sql,
        N'@LakehouseName sysname',
        @LakehouseName = @LakehouseName
END

GO

