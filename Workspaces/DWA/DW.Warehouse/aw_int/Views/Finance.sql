-- Auto Generated (Do not modify) 21AA21A82C92B96AEC9FC877E72D97086DD9A28077D57C33B4BA0CB9BBC18D67

/* Description: AW FactFinance
   Example: EXEC dwa.usp_TableLoad NULL,7,NULL
   History: 
			19/02/2025 Created
*/
CREATE VIEW [aw_int].[Finance] AS 
SELECT CONVERT(VARCHAR(8),CONVERT(DATE,CONVERT(VARCHAR(10),f.[AccountDate])),112) AS DateKey
	, f.[AccountCode] AS [AccountCodeAlternateKey]
	, f.[AccountDate] as [Date]
	, CONVERT(VARCHAR(50), f.[OrganizationName]) AS OrganizationName
	, CONVERT(VARCHAR(50), f.[DepartmentGroupName]) AS DepartmentGroupName
	, CONVERT(VARCHAR(50), f.ScenarioName) AS ScenarioName
	, SUM(CONVERT(DECIMAL(20, 6), isnull(f.[Amount], 0))) AS Amount
	, CONVERT(VARCHAR(512), f.[FileName]) AS FileName
	, ISNULL(CONVERT(VARCHAR(36), f.LineageKey), 0) AS  LineageKey
FROM LH.aw_stg.transactions f
GROUP BY [AccountDate],[AccountCode],[OrganizationName],[DepartmentGroupName],ScenarioName,f.LineageKey, f.FileName