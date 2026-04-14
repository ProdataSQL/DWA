-- Auto Generated (Do not modify) A7795A295B1BCC8A88D543CF7A07B6DC68EE6441858B6F77B31E9F6FA7611ED6

/* Description: AW Dimension Organization
   Example: EXEC dwa.usp_TableLoad NULL,5,NULL
   History: 
			19/02/2025 Created
*/
CREATE    VIEW [aw_int].[Organization] AS 
SELECT CONVERT(varchar(36), c.ParentOrganizationKey) AS ParentOrganizationKey
	, ISNULL(CONVERT(VARCHAR(10), PercentageOfOwnership), 0) AS PercentageOfOwnership
	, ISNULL(CONVERT(VARCHAR(50), p.OrganizationName), '') AS OrganizationName
	, ISNULL(CONVERT(VARCHAR(36), cu.CurrencyKey), 0) AS CurrencyKey
	, CONVERT(VARCHAR(512), p.[FileName]) AS FileName
	,ISNULL(CONVERT(VARCHAR(36),p.LineageKey),0)  AS  LineageKey
FROM LH.aw_stg.Organization p
LEFT JOIN (SELECT CONVERT(uniqueidentifier,HASHBYTES('MD5', OrganizationName),2) AS ParentOrganizationKey, OrganizationName FROM LH.aw_stg.Organization) c 
ON c.OrganizationName=p.ParentOrganizationName
INNER JOIN aw.DimCurrency cu ON cu.CurrencyAlternateKey=p.CurrencyCode COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8;