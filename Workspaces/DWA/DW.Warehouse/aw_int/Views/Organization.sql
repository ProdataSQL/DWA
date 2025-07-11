-- Auto Generated (Do not modify) F2AC694AB10BC94E381E64BD97B30BD1A7468C0D8998436EF1A14C54FD7457E9


/* Description: AW Dimension Organization
   Example: EXEC dwa.usp_TableLoad NULL,5,NULL
   History: 
			19/02/2025 Created
*/
CREATE   VIEW [aw_int].[Organization] AS 
SELECT ISNULL(CONVERT(VARCHAR(16),HASHBYTES('MD5', p.OrganizationName),2),'') AS OrganizationKey
	, CONVERT(VARCHAR(16), c.ParentOrganizationKey) AS ParentOrganizationKey
	, ISNULL(CONVERT(VARCHAR(10), PercentageOfOwnership), 0) AS PercentageOfOwnership
	, ISNULL(CONVERT(VARCHAR(50), p.OrganizationName), '') AS OrganizationName
	, ISNULL(CONVERT(VARCHAR(16), cu.CurrencyKey), 0) AS CurrencyKey
	, CONVERT(VARCHAR(512), p.[FileName]) AS FileName
	,ISNULL(CONVERT(VARCHAR(36),p.LineageKey),0)  AS  LineageKey
FROM LH.aw_stg.organization p
LEFT JOIN (SELECT CONVERT(VARCHAR(16),HASHBYTES('MD5', OrganizationName),2) AS ParentOrganizationKey, OrganizationName FROM LH.aw_stg.organization) c 
ON c.OrganizationName=p.ParentOrganizationName
INNER JOIN aw.DimCurrency cu ON cu.CurrencyAlternateKey=p.CurrencyCode COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8;