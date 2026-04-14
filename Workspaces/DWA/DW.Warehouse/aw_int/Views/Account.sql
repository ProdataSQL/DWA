-- Auto Generated (Do not modify) AEC801FC5FF3AE48B3084BBE393F30AADAFA887B893238278288A56603C08E1C


/* Description: AW Dim Account
   Example: EXEC dwa.usp_TableLoad @TableID=6
   History: 
			19/02/2025 Created
			26/08/2025 Kristan uniqueidentifier
*/
CREATE   VIEW [aw_int].[Account] AS
SELECT  ISNULL(CONVERT(INT, a.AccountCode), 0) AS AccountCode
	, CONVERT(INT, a.ParentAccountCode) AS ParentAccountCode
	, ISNULL(CONVERT(VARCHAR(50), a.AccountDescription), '') AS AccountDescription
	, CONVERT(VARCHAR(50), a.AccountType) AS AccountType
	, ISNULL(CONVERT(VARCHAR(50), a.Operator), '') AS Operator
	, CONVERT(VARCHAR(50), a.CustomMembers) AS CustomMembers
	, ISNULL(CONVERT(VARCHAR(50), a.ValueType), '') AS ValueType
	, CONVERT(VARCHAR(200), a.CustomMemberOptions) AS CustomMemberOptions
	, CONVERT(VARCHAR(512), a.[FileName]) AS FileName
	,ISNULL(CONVERT(VARCHAR(36),LineageKey),0) AS LineageKey
FROM LH.aw_stg.[Account] a
LEFT JOIN (SELECT CONVERT(VARCHAR(36),HASHBYTES('MD5', CONVERT(VARCHAR(4),ParentAccountCode)),2) AS ParentAccountKey, AccountCode  FROM LH.aw_stg.Account) c	ON c.AccountCode = a.ParentAccountCode;