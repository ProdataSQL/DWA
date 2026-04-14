CREATE TABLE [aw].[DimOrganization] (

	[OrganizationKey] uniqueidentifier NOT NULL, 
	[ParentOrganizationKey] varchar(36) NULL, 
	[PercentageOfOwnership] varchar(10) NOT NULL, 
	[OrganizationName] varchar(50) NOT NULL, 
	[CurrencyKey] varchar(36) NOT NULL, 
	[FileName] varchar(512) NULL, 
	[LineageKey] varchar(36) NOT NULL, 
	[RowChecksum] int NOT NULL
);