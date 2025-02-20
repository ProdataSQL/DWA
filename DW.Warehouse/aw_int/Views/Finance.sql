-- Auto Generated (Do not modify) 7497EC6EFF791EAE461844CAA7C4B586EA89EA9B6639C2DF733823F90530758A

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
	, row_number() OVER (PARTITION BY [AccountDate], [DepartmentGroupName], [ScenarioName],[OrganizationName], [AccountCode] ORDER BY f.LineageKey DESC) AS RowVersionNo
	, ISNULL(CONVERT(BIGINT, BINARY_CHECKSUM([AccountDate], [AccountCode], [OrganizationName], [DepartmentGroupName], ScenarioName, SUM(convert(DECIMAL(16, 6), f.[Amount])))), 0) AS RowChecksum
FROM LH.aw_stg.transactions f
GROUP BY [AccountDate],[AccountCode],[OrganizationName],[DepartmentGroupName],ScenarioName,f.LineageKey, f.FileName