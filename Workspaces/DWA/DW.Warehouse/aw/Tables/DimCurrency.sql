CREATE TABLE [aw].[DimCurrency] (

	[CurrencyKey] varchar(16) NULL, 
	[CurrencyAlternateKey] char(3) NOT NULL, 
	[CurrencyName] varchar(50) NOT NULL, 
	[RowChecksum] bigint NOT NULL, 
	[FileName] varchar(512) NULL, 
	[LineageKey] varchar(36) NOT NULL
);