CREATE TABLE [aw].[DimDepartmentGroup] (

	[DepartmentGroupKey] uniqueidentifier NOT NULL, 
	[ParentDepartmentGroupKey] uniqueidentifier NULL, 
	[DepartmentGroupName] varchar(50) NOT NULL, 
	[FileName] varchar(512) NULL, 
	[LineageKey] varchar(36) NOT NULL, 
	[RowChecksum] int NOT NULL
);