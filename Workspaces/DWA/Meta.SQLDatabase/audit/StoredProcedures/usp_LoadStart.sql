

/*
Description:Start Tracking a new Load 
Example:		
	exec [audit].[usp_LoadStart]  null,1
History:
	03/08/2023 Deepak, Created
	14/08/2023 Shruti, Refactoring
*/
CREATE PROC [audit].[usp_LoadStart] @RunID [uniqueidentifier], @TableID [int] AS
BEGIN
	SET NOCOUNT ON;
	SET @RunID =coalesce(@RunID,newid())
		INSERT INTO [audit].[LoadLog]
		(   RunID
		   ,TableID
		   ,SourceObject
		   ,SchemaName
		   ,TableName
		   ,StartDate
		   ,StartDateTime
		   ,SourceType
		   ,Status)	
		SELECT @RunID
		      ,t.TableID
		      ,t.SourceObject
		      ,t.SchemaName
		      ,t.TableName
		      ,getdate()
		      ,getdate()
		      ,t.SourceType
		      ,'Started'
		FROM config.edwTables t 
		WHERE t.TableID=@TableID
END

GO

