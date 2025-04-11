CREATE TABLE [stg].[DimAccount_staging] (

	[AccountKey] varchar(16) NULL, 
	[ParentAccountKey] varchar(16) NULL, 
	[AccountCodeAlternateKey] int NOT NULL, 
	[ParentAccountCodeAlternateKey] int NULL, 
	[AccountDescription] varchar(50) NOT NULL, 
	[AccountType] varchar(50) NULL, 
	[Operator] varchar(50) NOT NULL, 
	[CustomMembers] varchar(50) NULL, 
	[ValueType] varchar(50) NOT NULL, 
	[CustomMemberOptions] varchar(200) NULL, 
	[RowChecksum] int NOT NULL, 
	[FileName] varchar(512) NULL, 
	[LineageKey] varchar(36) NOT NULL
);