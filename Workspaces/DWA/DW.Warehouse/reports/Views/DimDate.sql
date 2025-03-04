-- Auto Generated (Do not modify) 886B20EE65965E88EA0F7A2ACE4F2448BC0050530018A009D29871EAE931728B



/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[DimDate]
AS
SELECT DateKey
	  ,FullDateAlternateKey AS Date
	  ,MonthName AS Month
	  ,FiscalQuarter AS [Fiscal Quarter]
	  ,FiscalYear AS [Fiscal Year]
	  ,MonthNumberOfYear AS [Month No]
	  ,FiscalYear * 100  + MonthNumberOfYear as [Fiscal Period No]
	  ,CONVERT (varchar(4), FiscalYear) + '-' + format(MonthNumberOfYear,'00') as [Fiscal Period] 
FROM aw.DimDate