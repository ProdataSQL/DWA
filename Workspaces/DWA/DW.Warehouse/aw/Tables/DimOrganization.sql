CREATE TABLE [aw].[DimOrganization] (

	[OrganizationKey] varchar(16) NOT NULL, 
	[ParentOrganizationKey] varchar(16) NULL, 
	[PercentageOfOwnership] varchar(10) NOT NULL, 
	[OrganizationName] varchar(50) NOT NULL, 
	[CurrencyKey] varchar(16) NOT NULL, 
	[RowChecksum] bigint NOT NULL, 
	[FileName] varchar(512) NULL, 
	[LineageKey] varchar(36) NOT NULL
);