-- Auto Generated (Do not modify) 62926774E06E2413D78F6491BEAB8E6D6F7FFD54237DC5D1D73D6B7D212965D4




/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[DimAccount]
AS
	WITH AccPath AS
	(
		SELECT TRIM('|' FROM COALESCE(a3.AccountDescription, '') + '|' + COALESCE(a2.AccountDescription, '') + '|' + COALESCE(a1.AccountDescription, '')) AS AccountPath 
		      ,a1.AccountKey, a1.ParentAccountKey, a1.AccountCodeAlternateKey, a1.ParentAccountCodeAlternateKey, a1.AccountDescription, a1.AccountType, a1.Operator, a1.CustomMembers, a1.ValueType, a1.CustomMemberOptions
		FROM aw.DimAccount a1
		LEFT JOIN aw.DimAccount a2
			   ON a1.ParentAccountKey = a2.AccountKey
		LEFT JOIN aw.DimAccount a3
			   ON a2.ParentAccountKey = a3.AccountKey
	),
	DelPos AS 
	(
		SELECT CHARINDEX('|', AccountPath) AS Delimiter1
		      ,CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath) + 1) AS Delimiter2
			  ,CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath) + 1) + 1) AS Delimiter3
			  ,AccountPath, AccountKey, ParentAccountKey, AccountCodeAlternateKey, ParentAccountCodeAlternateKey, AccountDescription, AccountType, Operator, CustomMembers, ValueType, CustomMemberOptions
		FROM AccPath
	)
	SELECT AccountKey, ParentAccountKey
	      ,AccountCodeAlternateKey AS [Account Code]
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