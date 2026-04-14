-- Auto Generated (Do not modify) 543BD044F6E3C439C92801252CB617812E2892571E0CDD9A7C534F5424239CD5


/* Description: AW FactFinance
   Example: EXEC dwa.usp_TableLoad NULL,7,NULL
   History: 
			19/02/2025 Created
*/
CREATE   VIEW [aw_int].[Finance] AS 
SELECT CONVERT(VARCHAR(8),CONVERT(DATE,CONVERT(VARCHAR(10),f.[AccountDate])),112) AS DateKey
	, f.[AccountCode] AS [AccountCode]
	, f.[AccountDate] as [Date]
	, CONVERT(VARCHAR(50), f.[OrganizationName]) AS OrganizationName
	, CONVERT(VARCHAR(50), f.[DepartmentGroupName]) AS DepartmentGroupName
	, CONVERT(VARCHAR(50), f.ScenarioName) AS ScenarioName
	, SUM(CONVERT(DECIMAL(20, 6), isnull(f.[Amount], 0))) AS Amount
	, CONVERT(VARCHAR(512), f.[FileName]) AS FileName
	, ISNULL(CONVERT(VARCHAR(36), f.LineageKey), 0) AS  LineageKey
FROM LH.aw_stg.Transactions f
GROUP BY [AccountDate],[AccountCode],[OrganizationName],[DepartmentGroupName],ScenarioName,f.LineageKey, f.FileName