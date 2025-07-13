CREATE TABLE [config].[Templates] (
    [Template]            VARCHAR (200)  NOT NULL,
    [TemplateDescription] VARCHAR (500)  NULL,
    [SourceSettings]      VARCHAR (8000) NULL,
    [TargetSettings]      VARCHAR (8000) NULL,
    [ArtefactType]        VARCHAR (100)  NOT NULL,
    [ConfigurationID]     INT            NOT NULL,
    CONSTRAINT [PK_Templates] PRIMARY KEY NONCLUSTERED ([Template] ASC)
);


GO

CREATE TRIGGER [config].TR_Templates
   ON  config.Templates
   AFTER INSERT,DELETE,UPDATE
AS 
BEGIN
	SET NOCOUNT ON;
	exec  [config].[usp_PipelineBuildMetaData]

END

GO

