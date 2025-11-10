CREATE TABLE [config].[Pipelines] (
    [PipelineID]               INT            NOT NULL,
    [Pipeline]                 VARCHAR (200)  NULL,
    [PipelineGroupID]          INT            NULL,
    [PipelineSequence]         INT            NULL,
    [SourceSettings]           VARCHAR (8000) NULL,
    [TargetSettings]           VARCHAR (8000) NULL,
    [ActivitySettings]         VARCHAR (8000) NULL,
    [Enabled]                  INT            CONSTRAINT [DF_Pipelines_Enabled] DEFAULT ((1)) NOT NULL,
    [PackageGroup]             VARCHAR (50)   NULL,
    [TableID]                  SMALLINT       NULL,
    [Stage]                    VARCHAR (50)   NULL,
    [PostExecuteSQL]           VARCHAR (8000) NULL,
    [Template]                 VARCHAR (200)  NULL,
    [PreExecuteSQL]            VARCHAR (8000) NULL,
    [SourceConnectionSettings] VARCHAR (8000) NULL,
    [TargetConnectionSettings] VARCHAR (8000) NULL,
    [SourceConfigurationID]    INT            NULL,
    [TargetConfigurationID]    INT            NULL,
    [ContinueOnError]          VARCHAR (8000) NULL,
    [Comments]                 VARCHAR (200)  NULL,
    [SessionTag]               VARCHAR (50)   NULL,
    [LakehouseConfigurationID] INT            NULL,
    [PromptID]                 INT            NULL,
    CONSTRAINT [PK_Pipelines] PRIMARY KEY CLUSTERED ([PipelineID] ASC),
    CONSTRAINT [FK_Pipelines_Configurations] FOREIGN KEY ([SourceConfigurationID]) REFERENCES [config].[Configurations] ([ConfigurationID]),
    CONSTRAINT [FK_Pipelines_PackageGroups] FOREIGN KEY ([PackageGroup]) REFERENCES [config].[PackageGroups] ([PackageGroup]),
    CONSTRAINT [FK_Pipelines_PipelineGroups] FOREIGN KEY ([PipelineGroupID]) REFERENCES [config].[PipelineGroups] ([PipelineGroupID]),
    CONSTRAINT [FK_Pipelines_Templates] FOREIGN KEY ([Template]) REFERENCES [config].[Templates] ([Template])
);


GO

CREATE TRIGGER [config].[TR_Pipelines]
   ON  [config].[Pipelines]
   AFTER INSERT,DELETE,UPDATE
AS 
BEGIN
	SET NOCOUNT ON;

	UPDATE p
	SET p.ActivitySettings = ai.ActivitySettings
	FROM config.Pipelines p
	INNER JOIN inserted i on i.PipelineID=p.PipelineID
	INNER JOIN config.aiPrompts ai on ai.PromptID = p.PromptID

	exec  [config].[usp_PipelineBuildMetaData]

END

GO

