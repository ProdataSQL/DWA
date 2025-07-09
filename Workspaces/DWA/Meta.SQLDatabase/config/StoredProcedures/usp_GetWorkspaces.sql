
/*
	Return Workspaces
*/
CREATE   PROCEDURE [config].[usp_GetWorkspaces]
AS
BEGIN
    SET NOCOUNT ON
	SELECT DISTINCT JSON_VALUE([ConnectionSettings], '$.WorkspaceID') AS WorkspaceID
	FROM config.Configurations
	WHERE JSON_VALUE([ConnectionSettings], '$.WorkspaceID')  is not null 
END

GO

