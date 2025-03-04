CREATE TABLE [config].[Configurations] (
    [ConfigurationID]    INT            NOT NULL,
    [ConfigurationName]  VARCHAR (8000) NULL,
    [ConnectionSettings] VARCHAR (8000) NULL,
    [Enabled]            INT            NULL,
    CONSTRAINT [PK_Configurations] PRIMARY KEY CLUSTERED ([ConfigurationID] ASC)
);


GO


CREATE TRIGGER [config].TR_Configurations 
   ON  [config].[Configurations]
   AFTER INSERT,DELETE,UPDATE
AS 
BEGIN
	SET NOCOUNT ON;
	exec  [config].[usp_PipelineBuildMetaData]

END

GO

