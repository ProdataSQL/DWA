CREATE TABLE [config].[PipelineTables] (
    [PipelineID]     INT           NOT NULL,
    [TableID]        SMALLINT      NOT NULL,
    [TargetSchema]   VARCHAR (128) NULL,
    [TargetTable]    VARCHAR (128) NULL,
    [TargetDatabase] VARCHAR (128) NULL,
    CONSTRAINT [PK_PipelineTables] PRIMARY KEY NONCLUSTERED ([PipelineID] ASC, [TableID] ASC),
    CONSTRAINT [FK_PipelineTables_edwTables] FOREIGN KEY ([TableID]) REFERENCES [config].[edwTables] ([TableID]),
    CONSTRAINT [FK_PipelineTables_Pipelines] FOREIGN KEY ([PipelineID]) REFERENCES [config].[Pipelines] ([PipelineID])
);


GO

