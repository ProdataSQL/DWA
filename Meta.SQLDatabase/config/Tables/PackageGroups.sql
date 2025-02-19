CREATE TABLE [config].[PackageGroups] (
    [PackageGroup] VARCHAR (50) NOT NULL,
    [Monitored]    BIT          NULL,
    [SortOrder]    VARCHAR (50) NULL,
    CONSTRAINT [PK_PackageGroups] PRIMARY KEY NONCLUSTERED ([PackageGroup] ASC)
);


GO



CREATE TRIGGER [config].TR_PackageGroups
   ON  [config].[PackageGroups]
   AFTER INSERT,DELETE,UPDATE
AS 
BEGIN
	SET NOCOUNT ON;
	exec  [config].[usp_PipelineBuildMetaData]

END

GO

