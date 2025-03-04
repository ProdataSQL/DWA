CREATE TABLE [config].[PipelineGroups] (
    [PipelineGroupID]          INT            NOT NULL,
    [PipelineGroupName]        VARCHAR (200)  NULL,
    [PipelineGroupDesc]        VARCHAR (500)  NULL,
    [Template]                 VARCHAR (200)  NULL,
    [SourceSettings]           VARCHAR (8000) NULL,
    [TargetSettings]           VARCHAR (8000) NULL,
    [Enabled]                  INT            NULL,
    [ActivitySettings]         VARCHAR (8000) NULL,
    [SourceConfigurationID]    INT            NULL,
    [TargetConfigurationID]    INT            NULL,
    [PackageGroup]             VARCHAR (50)   NULL,
    [Stage]                    VARCHAR (8000) NULL,
    [PostExecuteSQL]           VARCHAR (8000) NULL,
    [PreExecuteSQL]            VARCHAR (8000) NULL,
    [SourceConnectionSettings] VARCHAR (8000) NULL,
    [TargetConnectionSettings] VARCHAR (8000) NULL,
    [ContinueOnError]          VARCHAR (200)  NULL,
    CONSTRAINT [PK_PipelineGroups] PRIMARY KEY NONCLUSTERED ([PipelineGroupID] ASC),
    CONSTRAINT [FK_PipelineGroups_Configurations] FOREIGN KEY ([SourceConfigurationID]) REFERENCES [config].[Configurations] ([ConfigurationID]),
    CONSTRAINT [FK_PipelineGroups_Configurations1] FOREIGN KEY ([TargetConfigurationID]) REFERENCES [config].[Configurations] ([ConfigurationID]),
    CONSTRAINT [FK_PipelineGroups_Templates] FOREIGN KEY ([Template]) REFERENCES [config].[Templates] ([Template])
);


GO



CREATE TRIGGER [config].TR_PipelineGroups
   ON  [config].[PipelineGroups]
   AFTER INSERT,DELETE,UPDATE
AS 
BEGIN
	SET NOCOUNT ON;
	exec  [config].[usp_PipelineBuildMetaData]

END

GO

