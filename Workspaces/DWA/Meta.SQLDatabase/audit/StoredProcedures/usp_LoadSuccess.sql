
/*
Description:	Mark Load as Successful
Example:		exec [audit].[usp_LoadSuccess] NULL, 1 
History:		
		14/08/2023 Shruti, Created
*/
CREATE PROC [audit].[usp_LoadSuccess] @RunID uniqueidentifier, @TableID [int] AS
BEGIN
	SET NOCOUNT ON;
	INSERT INTO [audit].[LoadLog]
	(
		RunID
	   ,TableID
	   ,SourceObject
	   ,SchemaName
	   ,TableName
	   ,StartDate
	   ,StartDateTime
	   ,SourceType
	   ,Status
	)	
	SELECT COALESCE(@RunID,newId())	
		  ,t.TableID
		  ,t.SourceObject
		  ,t.SchemaName
		  ,t.TableName
		  ,GETDATE()
		  ,GETDATE()
		  ,t.SourceType
		  ,'Completed'
	FROM config.edwTables t 
	WHERE t.TableID=@TableID

END

GO

