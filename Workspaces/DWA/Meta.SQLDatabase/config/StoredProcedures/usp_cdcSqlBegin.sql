SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*
    Begin SQL Server CDC and Return a List of Tables for CDC 
    Log Target LSN amd Traget Datetime 
    Test: 
        exec config.usp_cdcSqlBegin
    History:    15/06/2025 Bob, Created   
                10/07/2025 Kristan, Added ConfigurationID

*/
CREATE PROCEDURE [config].[usp_cdcSqlBegin]
(
    @ConnectionID varchar(50) = '8a10eff2-cdcc-4b00-a2ef-35bae56d7ef2',
    @Database sysname = 'HICPStoreOne',
    @target_lsn varchar(50) = '0x0005E0CA0006EDD90040',
    @target_datetime varchar(50) = '2025-06-15T20:21:40.757Z',
    @Table sysname = null -- Optional if Just One Table
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @target_datetime2 datetime2(7);
    DECLARE @target_lsn_bin binary(10);
    DECLARE @ConfigurationID int;

    SELECT 
        @target_datetime2 = CONVERT(datetime2(2), @target_datetime),
        @target_lsn_bin = CONVERT(binary(10), @target_lsn, 1);

    -- Get ConfigurationID from config.Configuration using ConnectionID
    SELECT @ConfigurationID = ConfigurationID
    FROM config.Configurations
    WHERE JSON_VALUE(ConnectionSettings, '$.ConnectionID') = @ConnectionID;

   
    UPDATE config.cdcSqlTables
    SET target_lsn = @target_lsn_bin,
        target_datetime = @target_datetime2
    WHERE ConfigurationID = @ConfigurationID
      AND [Database] = @Database
      AND (@Table IS NULL OR [Table] = @Table)
      AND Enabled = 1;

    SELECT 
        [Database], 
        [Table], 
        CONVERT(varchar(50), max_lsn, 1) AS from_lsn,
        max_datetime AS from_datetime,
        CONVERT(varchar(50), target_lsn, 1) AS to_lsn,
        target_datetime AS to_datetime,
        PrimaryKeys
    FROM config.cdcSqlTables
    WHERE ConfigurationID = @ConfigurationID
      AND [Database] = @Database
      AND (@Table IS NULL OR [Table] = @Table)
      AND Enabled = 1;
END;