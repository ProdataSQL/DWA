CREATE TABLE [dbo].[dict_artefacts] (
    [id]           VARCHAR (MAX) NULL,
    [display_name] VARCHAR (MAX) NULL,
    [description]  VARCHAR (MAX) NULL,
    [type]         VARCHAR (MAX) NULL,
    [workspace_id] VARCHAR (MAX) NULL
);


GO




CREATE   TRIGGER [TR_dict_artefacts]
   ON  [dbo].[dict_artefacts]
   AFTER INSERT,DELETE,UPDATE
AS 
BEGIN
	SET NOCOUNT ON;
	exec  [config].[usp_PipelineBuildMetaData]

END

GO

