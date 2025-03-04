CREATE TABLE [config].[DatasetLineage] (
    [Dataset]          VARCHAR (50)  NOT NULL,
    [DatasetTableName] VARCHAR (128) NOT NULL,
    [SourceDatabase]   VARCHAR (128) NULL,
    [SourceObject]     VARCHAR (255) NULL,
    [SourceType]       VARCHAR (50)  NULL,
    [SourceConnection] VARCHAR (255) NULL,
    [PipelineID]       INT           NULL,
    CONSTRAINT [FK_DatasetLineage_Datasets] FOREIGN KEY ([Dataset]) REFERENCES [config].[Datasets] ([Dataset]),
    CONSTRAINT [FK_DatasetLineage_Pipelines] FOREIGN KEY ([PipelineID]) REFERENCES [config].[Pipelines] ([PipelineID])
);


GO

