


/*
Description:	Mark Load as Failed in case of Failure
Example:		exec [audit].[usp_LoadFailure] 2, 1, '1', NULL
				exec [audit].[usp_LoadFailure] '5CC853AC-BA18-438E-82A0-6621533B8728', 1, '1', 'Error Description 1'
History:		
		14/08/2023 Shruti, Created
*/
CREATE PROCEDURE [audit].[usp_LoadFailure] @RunID UNIQUEIDENTIFIER, @TableID [int], @ErrorCode [int], @ErrorDescription [nvarchar] (max) AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @ExecutionKey INT
	DECLARE @ErrorCount INT
	DECLARE @ErrorCountLoad INT
	DECLARE @SourceObject NVARCHAR(4000)

	IF LEN(@ErrorDescription) = 0
		SELECT @ErrorDescription = 'Load Failed. Check Logs'
			, @ErrorCode = - 1

	INSERT INTO [audit].[LoadLog] (
		RunID
		, TableID
		, SourceObject
		, SchemaName
		, TableName
		, StartDate
		, StartDateTime
		, SourceType
		, Status
		, ErrorCode
		, ErrorDescription
		, ErrorSource
		)
	SELECT COALESCE(@RunID,NULL) AS RunID
		, t.TableID
		, t.SourceObject
		, t.SchemaName
		, t.TableName
		, GETDATE()
		, GETDATE()
		, t.SourceType
		, 'Failed'
		, @ErrorCode
		, @ErrorDescription
		, t.SourceObject
	FROM config.edwTables t
	WHERE t.TableID = @TableID;
	
END

GO

