/*
    Return Workspaces
*/
CREATE    PROCEDURE [config].[usp_GetWorkspaces]
AS
BEGIN
    SET NOCOUNT ON
    SELECT DISTINCT JSON_VALUE(lower([ConnectionSettings]), lower('$.workspaceid')) AS WorkspaceID
    FROM config.Configurations
    WHERE JSON_VALUE(lower([ConnectionSettings]), lower('$.workspaceid')) is not null
END

GO

