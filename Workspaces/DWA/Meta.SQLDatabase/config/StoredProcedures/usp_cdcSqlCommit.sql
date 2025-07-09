/*
    Commit SQL Server CDC
     Test: 
        exec config.usp_cdcSqlCommit 'TableHComment'
    History:    15/06/2025 Bob, Created           
*/
CREATE   PROCEDURE [config].[usp_cdcSqlCommit]
(
    @Table sysname 

)
AS
BEGIN
    SET NOCOUNT ON
 
    UPDATE [config].[cdcSqlTables]
        SET max_lsn=target_lsn, max_datetime=target_datetime
    WHERE [Table] =@Table
END

GO

