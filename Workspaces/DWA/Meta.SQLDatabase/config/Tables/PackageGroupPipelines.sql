CREATE TABLE [config].[PackageGroupPipelines] (
    [PackageGroup] VARCHAR (50) NOT NULL,
    [PipelineID]   INT          NOT NULL,
    CONSTRAINT [PK_PackageGroupPipelines] PRIMARY KEY CLUSTERED ([PackageGroup] ASC, [PipelineID] ASC),
    CONSTRAINT [FK_PackageGroupPipelines_PackageGroups] FOREIGN KEY ([PackageGroup]) REFERENCES [config].[PackageGroups] ([PackageGroup]),
    CONSTRAINT [FK_PackageGroupPipelines_Pipelines] FOREIGN KEY ([PipelineID]) REFERENCES [config].[Pipelines] ([PipelineID])
);


GO

