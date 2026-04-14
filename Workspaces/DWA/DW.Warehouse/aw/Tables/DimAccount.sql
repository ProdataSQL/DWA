CREATE TABLE [aw].[DimAccount] (

	[AccountKey] uniqueidentifier NOT NULL, 
	[AccountCode] int NOT NULL, 
	[ParentAccountCode] int NULL, 
	[AccountDescription] varchar(50) NOT NULL, 
	[AccountType] varchar(50) NULL, 
	[Operator] varchar(50) NOT NULL, 
	[CustomMembers] varchar(50) NULL, 
	[ValueType] varchar(50) NOT NULL, 
	[CustomMemberOptions] varchar(200) NULL, 
	[FileName] varchar(512) NULL, 
	[LineageKey] varchar(36) NOT NULL, 
	[RowChecksum] int NOT NULL
);