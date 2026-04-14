-- Auto Generated (Do not modify) 1AABB97F8D82F6689105134EA772170FF9FAF026A353F3EA26E42D2C4A8F786C





/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			27/10/2023 Shruti, Created
			10/02/2026 Kristan, Removed extra key columns
*/
CREATE    VIEW [reports].[DimAccount]
AS
	WITH AccPath AS
	(
		SELECT TRIM('|' FROM COALESCE(a3.AccountDescription, '') + '|' + COALESCE(a2.AccountDescription, '') + '|' + COALESCE(a1.AccountDescription, '')) AS AccountPath 
		      ,a1.AccountCode, a1.AccountDescription, a1.AccountType, a1.Operator, a1.CustomMembers, a1.ValueType, a1.CustomMemberOptions
		FROM aw.DimAccount a1
		LEFT JOIN aw.DimAccount a2
			   ON a1.ParentAccountCode = a2.AccountCode
		LEFT JOIN aw.DimAccount a3
			   ON a2.ParentAccountCode = a3.AccountCode
	),
	DelPos AS 
	(
		SELECT CHARINDEX('|', AccountPath) AS Delimiter1
		      ,CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath) + 1) AS Delimiter2
			  ,CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath) + 1) + 1) AS Delimiter3
			  ,AccountPath, AccountCode, AccountDescription, AccountType, Operator, CustomMembers, ValueType, CustomMemberOptions
		FROM AccPath
	)
	SELECT AccountCode
		  ,AccountDescription AS Account
		  ,AccountType AS [Account Type], Operator
		  ,ValueType AS [Value Type]
	      ,CASE WHEN Delimiter1 = 0 THEN AccountPath 
		        ELSE SUBSTRING(AccountPath, 1, Delimiter1 - 1) 
		   END AS Report
		  ,CASE WHEN Delimiter1 = 0 THEN ''
				WHEN Delimiter2 = 0 THEN SUBSTRING(AccountPath, Delimiter1 + 1, LEN(AccountPath) - Delimiter1 )
				ELSE SUBSTRING(AccountPath, Delimiter1 + 1, Delimiter2 - Delimiter1 -1)
		   END AS [Account L2]
		  ,CASE WHEN Delimiter1 = 0 OR Delimiter2 = 0 THEN ''
		        WHEN Delimiter3 = 0 THEN SUBSTRING(AccountPath, Delimiter2 + 1, LEN(AccountPath) - Delimiter2)
				ELSE SUBSTRING(AccountPath, Delimiter2 + 1, Delimiter3 - Delimiter2 - 1)
		   END [Account L3]
	FROM DelPos