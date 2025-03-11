CREATE TABLE [audit].[ReleaseLog] (
    [ReleaseNo]         INT              NOT NULL,
    [SourceWorkspaceId] UNIQUEIDENTIFIER NOT NULL,
    [TargetWorkspaceId] UNIQUEIDENTIFIER NOT NULL,
    [SourceWorkspace]   VARCHAR (50)     NOT NULL,
    [TargetWorkspace]   VARCHAR (50)     NOT NULL,
    [ArtefactsJson]     VARCHAR (8000)   NULL,
    [SqlScripts]        VARCHAR (8000)   NULL,
    [ReleaseFolder]     VARCHAR (8000)   NOT NULL,
    [CreatedBy]         VARCHAR (8000)   NULL,
    [CreatedDateTime]   DATETIME2 (6)    NOT NULL,
    [Status]            VARCHAR (20)     NOT NULL,
    [ErrorMessage]      VARCHAR (8000)   NULL,
    [StartedTime]       DATETIME2 (6)    NULL,
    [CompletedTime]     DATETIME2 (6)    NULL,
    [ReleaseNotes]      VARCHAR (8000)   NULL,
    [ReleaseNotesFile]  VARCHAR (500)    NULL,
    PRIMARY KEY CLUSTERED ([ReleaseNo] ASC)
);


GO

