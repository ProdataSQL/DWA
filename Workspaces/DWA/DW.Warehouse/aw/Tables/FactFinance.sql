CREATE TABLE [aw].[FactFinance] (

	[DateKey] varchar(8) NOT NULL, 
	[DepartmentGroupKey] uniqueidentifier NOT NULL, 
	[ScenarioKey] uniqueidentifier NOT NULL, 
	[OrganizationKey] uniqueidentifier NOT NULL, 
	[AccountKey] uniqueidentifier NOT NULL, 
	[Date] varchar(8000) NULL, 
	[Amount] decimal(38,6) NULL, 
	[FileName] varchar(512) NULL, 
	[LineageKey] varchar(36) NOT NULL
);