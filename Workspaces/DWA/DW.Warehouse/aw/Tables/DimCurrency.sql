CREATE TABLE [aw].[DimCurrency] (

	[CurrencyKey] uniqueidentifier NOT NULL, 
	[CurrencyAlternateKey] char(3) NOT NULL, 
	[CurrencyName] varchar(50) NOT NULL, 
	[FileName] varchar(512) NULL, 
	[LineageKey] varchar(36) NOT NULL, 
	[RowChecksum] int NOT NULL
);