-- Auto Generated (Do not modify) 4A407AEA23D0495B7E52EB228A4636AFF639BDB0402989BBD35D9F9ADD0FD70A
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
	--, ISNULL(CONVERT(VARCHAR(16), cu.CurrencyKey), 0) AS CurrencyKey
	, ISNULL(CONVERT(bigint,BINARY_CHECKSUM(ParentOrganizationKey,p.OrganizationName, p.OrganizationName)),0) AS RowChecksum
	, CONVERT(VARCHAR(512), p.[FileName]) AS FileName
	,ISNULL(CONVERT(VARCHAR(36),p.LineageKey),0)  AS  LineageKey
FROM LH.aw_stg.organization p
LEFT JOIN (SELECT CONVERT(VARCHAR(16),HASHBYTES('MD5', OrganizationName),2) AS ParentOrganizationKey, OrganizationName FROM LH.aw_stg.organization) c 
ON c.OrganizationName=p.ParentOrganizationName
--INNER JOIN aw.DimCurrency cu ON cu.CurrencyAlternateKey=p.CurrencyCode;