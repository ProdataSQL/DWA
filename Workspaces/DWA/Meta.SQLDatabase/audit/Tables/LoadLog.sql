CREATE TABLE [audit].[LoadLog] (
    [RunID]            UNIQUEIDENTIFIER NULL,
    [TableID]          SMALLINT         NOT NULL,
    [SchemaName]       VARCHAR (128)    NOT NULL,
    [TableName]        VARCHAR (128)    NOT NULL,
    [SourceObject]     VARCHAR (128)    NOT NULL,
    [SourceType]       VARCHAR (10)     NOT NULL,
    [StartDate]        DATE             NOT NULL,
    [StartDateTime]    DATETIME2 (6)    NOT NULL,
    [Status]           VARCHAR (20)     NOT NULL,
    [ErrorCode]        VARCHAR (4000)   NULL,
    [ErrorDescription] VARCHAR (4000)   NULL,
    [ErrorSource]      VARCHAR (4000)   NULL
);


GO

