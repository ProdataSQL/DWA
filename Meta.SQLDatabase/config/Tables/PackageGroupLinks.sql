CREATE TABLE [config].[PackageGroupLinks] (
    [PackageGroup]      VARCHAR (50) NOT NULL,
    [ChildPackageGroup] VARCHAR (50) NOT NULL,
    CONSTRAINT [PK_PackageGroupLinks] PRIMARY KEY NONCLUSTERED ([PackageGroup] ASC, [ChildPackageGroup] ASC),
    CONSTRAINT [FK_PackageGroupLinks_PackageGroups] FOREIGN KEY ([PackageGroup]) REFERENCES [config].[PackageGroups] ([PackageGroup]),
    CONSTRAINT [FK_PackageGroupLinks_PackageGroups1] FOREIGN KEY ([ChildPackageGroup]) REFERENCES [config].[PackageGroups] ([PackageGroup])
);


GO

