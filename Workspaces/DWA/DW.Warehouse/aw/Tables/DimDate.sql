CREATE TABLE [aw].[DimDate] (

	[DateKey] varchar(8) NOT NULL, 
	[FullDateAlternateKey] date NOT NULL, 
	[DayNumberOfWeek] smallint NOT NULL, 
	[DayOfWeek] varchar(50) NOT NULL, 
	[DayNumberOfMonth] smallint NOT NULL, 
	[DayNumberOfYear] int NOT NULL, 
	[WeekNumberOfYear] int NOT NULL, 
	[MonthName] varchar(50) NOT NULL, 
	[MonthNumberOfYear] smallint NOT NULL, 
	[CalendarQuarter] smallint NOT NULL, 
	[CalendarYear] smallint NOT NULL, 
	[CalendarSemester] smallint NOT NULL, 
	[FiscalQuarter] smallint NOT NULL, 
	[FiscalYear] smallint NOT NULL, 
	[FiscalSemester] smallint NOT NULL, 
	[FileName] varchar(512) NULL, 
	[LineageKey] varchar(36) NOT NULL
);