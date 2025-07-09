/*
    Begin SQL Server CDC and Return a List of Tables for CDC 
    Log Target LSN amd Traget Datetime 
    Test: 
        exec config.usp_cdcSqlBegin
    History:    15/06/2025 Bob, Created           

*/
CREATE   PROCEDURE [config].[usp_cdcSqlBegin]
(
    @ConnectionID varchar(50)	='8a10eff2-cdcc-4b00-a2ef-35bae56d7ef2'
	,@Database sysname			='HICPStoreOne'
    ,@target_lsn varchar(50)	='0x0005E0CA0006EDD90040'
    ,@target_datetime varchar(50) =  '2025-06-15T20:21:40.757Z'
	,@Table sysname				=null /* Optional if Just One Table */
)
AS
BEGIN
    SET NOCOUNT ON
    DECLARE @target_datetime2 datetime2(7)
    DECLARE @target_lsn_bin binary(10) 
    SELECT @target_datetime2= CONVERT(datetime2(2),@target_datetime), @target_lsn_bin = CONVERT (binary(10),  @target_lsn,1)

	UPDATE config.cdcSqlTables
	SET target_lsn      = @target_lsn_bin,	target_datetime = @target_datetime2
	WHERE ConnectionID=@ConnectionID
	AND [Database]=@Database
	and ([Table]=@Table or @Table is null)
	and Enabled=1


	SELECT [Database], [Table], convert(varchar(50),max_lsn,1) as from_lsn , max_datetime as from_datetime, convert(varchar(50),target_lsn,1) as to_lsn, target_datetime as to_datetime, PrimaryKeys
	FROM config.cdcSqlTables
	WHERE ConnectionID=@ConnectionID
	AND [Database]=@Database
	and ([Table]=@Table or @Table is null)
	and Enabled=1

  
END

GO

