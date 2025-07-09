CREATE TABLE [audit].[PipelineLog] (
    [LineageKey]       UNIQUEIDENTIFIER NOT NULL,
    [RunID]            UNIQUEIDENTIFIER NULL,
    [PipelineID]       INT              NOT NULL,
    [PackageGroup]     VARCHAR (50)     NULL,
    [PipelineName]     VARCHAR (512)    NULL,
    [Status]           VARCHAR (100)    NOT NULL,
    [StartDate]        DATE             NOT NULL,
    [StartDateTime]    DATETIME2 (6)    NOT NULL,
    [EndDateTime]      DATETIME2 (6)    NULL,
    [Template]         VARCHAR (200)    NULL,
    [StartedBy]        VARCHAR (512)    NULL,
    [PipelineSequence] SMALLINT         NULL,
    [ErrorCode]        VARCHAR (4000)   NULL,
    [ErrorMessage]     VARCHAR (4000)   NULL,
    [Stage]            VARCHAR (20)     NULL,
    [Pipeline]         UNIQUEIDENTIFIER NULL,
    [ParentRunID]      UNIQUEIDENTIFIER NULL,
    [ParentGroupID]    UNIQUEIDENTIFIER NULL,
    [WorkspaceID]      UNIQUEIDENTIFIER NULL,
    [MonitoringUrl]    VARCHAR (512)    NULL,
    [RowsRead]         BIGINT           NULL,
    [CopyDuration]     INT              NULL,
    [DataRead]         BIGINT           NULL,
    [DataWritten]      BIGINT           NULL,
    [ExtendedLog]      VARCHAR (8000)   NULL,
    [DebugOutput]      VARCHAR (8000)   NULL,
    CONSTRAINT [PK_PipelineLog] PRIMARY KEY CLUSTERED ([LineageKey] ASC)
);


GO

