CREATE TABLE [aw].[FactFinance] (

	[DateKey] varchar(8) NOT NULL, 
	[DepartmentGroupKey] varchar(16) NULL, 
	[ScenarioKey] varchar(16) NOT NULL, 
	[OrganizationKey] varchar(16) NOT NULL, 
	[AccountKey] varchar(16) NULL, 
	[Date] varchar(8000) NULL, 
	[Amount] decimal(38,6) NULL, 
	[FileName] varchar(512) NULL, 
	[LineageKey] varchar(36) NOT NULL, 
	[RowChecksum] bigint NOT NULL
);

