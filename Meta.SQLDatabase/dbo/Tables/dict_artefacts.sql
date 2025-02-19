CREATE TABLE [dbo].[dict_artefacts] (
    [id]           NVARCHAR (MAX) NULL,
    [display_name] NVARCHAR (MAX) NULL,
    [description]  NVARCHAR (MAX) NULL,
    [type]         NVARCHAR (MAX) NULL,
    [workspace_id] NVARCHAR (MAX) NULL
);


GO



CREATE TRIGGER TR_artefacts
   ON  [dbo].[dict_artefacts]
   AFTER INSERT,DELETE,UPDATE
AS 
BEGIN
	SET NOCOUNT ON;
	exec  [config].[usp_PipelineBuildMetaData]

END

GO

