-- Auto Generated (Do not modify) B657F59F0F5A0E30FD6BB145732616630A968CCA73A651989934E1B37DE99677





/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[FinancialReport]
AS
	SELECT ReportNo AS [Report No]
	      ,ReportHeading AS [Report Heading]
		  ,ReportSection AS [Report Section]
		  ,[LineNo]
		  ,LinkLineNo
		  ,LinkReport
		  ,LinkHeading
		  
		  ,ReportHeadingNo
		  ,COALESCE(LinkSignage, 1) AS [Link Signage]
		  ,Operator
		  ,Report
		  ,Calc1
	FROM aw_int.AccountRange