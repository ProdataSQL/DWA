-- Auto Generated (Do not modify) EA147841DFF905BE86CB6B0C0B5CB27470E39EDC45FE607472A2CF6C94BD31EE



/* Description: AW Dim Account
   Example: EXEC dwa.usp_TableLoad @TableID=6
   History: 
			19/02/2025 Shruti Created
*/
CREATE VIEW [aw_int].[Account] AS
SELECT  CONVERT(VARCHAR(16),HASHBYTES('MD5', CONVERT(VARCHAR(4),a.AccountCode)),2) AS AccountKey
	, CONVERT(VARCHAR(16), c.ParentAccountKey) AS ParentAccountKey
	, ISNULL(CONVERT(INT, a.AccountCode), 0) AS AccountCodeAlternateKey
	, CONVERT(INT, a.ParentAccountCode) AS ParentAccountCodeAlternateKey
	, ISNULL(CONVERT(VARCHAR(50), a.AccountDescription COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8), '') AS AccountDescription
	, CONVERT(VARCHAR(50), a.AccountType COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8) AS AccountType
	, ISNULL(CONVERT(VARCHAR(50), a.Operator COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8), '') AS Operator
	, CONVERT(VARCHAR(50), a.CustomMembers COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8) AS CustomMembers
	, ISNULL(CONVERT(VARCHAR(50), a.ValueType COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8), '') AS ValueType
	, CONVERT(VARCHAR(200), a.CustomMemberOptions COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8) AS CustomMemberOptions
	, CONVERT(VARCHAR(512), a.[FileName] COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8) AS FileName
	,ISNULL(CONVERT(VARCHAR(36),LineageKey COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8),0) AS LineageKey
FROM LH.aw_stg.[account] a
LEFT JOIN (SELECT CONVERT(VARCHAR(16),HASHBYTES('MD5', CONVERT(VARCHAR(4),ParentAccountCode)),2) AS ParentAccountKey, AccountCode  FROM LH.aw_stg.account) c	ON c.AccountCode = a.ParentAccountCode;