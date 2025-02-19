CREATE TABLE [config].[edwTableLineage] (
    [TableID]        INT           NULL,
    [SourceTable]    VARCHAR (128) NULL,
    [SourceType]     VARCHAR (128) NULL,
    [SourceDatabase] VARCHAR (128) NULL,
    [PipelineID]     INT           NULL
);


GO

