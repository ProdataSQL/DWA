CREATE TABLE [aw].[DimDepartmentGroup] (

	[DepartmentGroupKey] varchar(16) NULL, 
	[ParentDepartmentGroupKey] varchar(16) NULL, 
	[DepartmentGroupName] varchar(50) NOT NULL, 
	[FileName] varchar(512) NULL, 
	[LineageKey] varchar(36) NOT NULL, 
	[RowChecksum] int NOT NULL
);