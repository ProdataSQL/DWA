CREATE TABLE [config].[PackageGroupTables] (
    [PackageGroup] VARCHAR (50) NOT NULL,
    [TableID]      SMALLINT     NOT NULL,
    CONSTRAINT [PK_PackageGroupTables] PRIMARY KEY CLUSTERED ([PackageGroup] ASC, [TableID] ASC),
    CONSTRAINT [FK_PackageGroupTables_edwTables] FOREIGN KEY ([TableID]) REFERENCES [config].[edwTables] ([TableID]),
    CONSTRAINT [FK_PackageGroupTables_PackageGroups] FOREIGN KEY ([PackageGroup]) REFERENCES [config].[PackageGroups] ([PackageGroup])
);


GO

