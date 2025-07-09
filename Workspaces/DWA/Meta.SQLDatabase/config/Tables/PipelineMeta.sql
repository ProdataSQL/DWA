CREATE TABLE [config].[PipelineMeta] (
    [PipelineID]               INT              NULL,
    [Pipeline]                 VARCHAR (200)    NULL,
    [Template]                 VARCHAR (128)    NULL,
    [SourceConnectionSettings] VARCHAR (8000)   NULL,
    [TargetConnectionSettings] VARCHAR (8000)   NULL,
    [SourceSettings]           VARCHAR (8000)   NULL,
    [TargetSettings]           VARCHAR (8000)   NULL,
    [ActivitySettings]         VARCHAR (8000)   NULL,
    [PipelineSequence]         INT              NOT NULL,
    [PackageGroup]             VARCHAR (8000)   NULL,
    [PreExecuteSQL]            VARCHAR (8000)   NULL,
    [PostExecuteSQL]           VARCHAR (8000)   NULL,
    [Enabled]                  BIT              NOT NULL,
    [Stage]                    VARCHAR (50)     NULL,
    [TemplateType]             VARCHAR (50)     NULL,
    [TemplateID]               UNIQUEIDENTIFIER NULL,
    [TableID]                  INT              NULL,
    [SessionTag]               VARCHAR (50)     NULL,
    [TemplateWorkspaceID]      UNIQUEIDENTIFIER NULL
);


GO

