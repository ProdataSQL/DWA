/*
    Return a List of Tables for CDC and their Primary Keys
    Used By: Extract-Parquet-CDC Notebook
    Test: 
        exec [config].[usp_cdcSqlTables] @LakehouseName='StageDM2'
    History:    15/06/2025 Bob, Created           

*/
CREATE     PROCEDURE [config].[usp_cdcSqlTables]
(
	@LakehouseName sysname ='StageDM2'
)
AS
BEGIN
    SET NOCOUNT ON
  
    SELECT [Table], PrimaryKeys  FROM config.cdcSqlTables where LakehouseName =@LakehouseName and Enabled=1
END

GO

