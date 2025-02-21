-- Auto Generated (Do not modify) F70E5A83F1A2D3583F6C5E522F82F689A99D8F217E02FAF0E470C159B2F3FE2D




/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[DimOrganization]
AS
	WITH OrgPath AS
	(
		SELECT TRIM('|' FROM COALESCE(o4.OrganizationName, '') + '|' + COALESCE(o3.OrganizationName, '') + '|' + COALESCE(o2.OrganizationName, '') + '|' + COALESCE(o1.OrganizationName, '')) AS OrganizationPath,
			o1.OrganizationKey, o1.ParentOrganizationKey, o1.PercentageOfOwnership, o1.OrganizationName, o1.CurrencyKey
		FROM
		    aw.DimOrganization o1
		LEFT JOIN
		    aw.DimOrganization o2 ON o1.ParentOrganizationKey = o2.OrganizationKey
		LEFT JOIN
		    aw.DimOrganization o3 ON o2.ParentOrganizationKey = o3.OrganizationKey
		LEFT JOIN
		    aw.DimOrganization o4 ON o3.ParentOrganizationKey = o4.OrganizationKey
	),
	DelPos AS
	(
		SELECT CHARINDEX('|', OrganizationPath) AS Delimiter1
		      ,CHARINDEX('|', OrganizationPath, CHARINDEX('|', OrganizationPath) + 1) AS Delimiter2
			  ,CHARINDEX('|', OrganizationPath, CHARINDEX('|', OrganizationPath, CHARINDEX('|', OrganizationPath) + 1) + 1) AS Delimiter3
			  ,OrganizationPath, OrganizationKey, ParentOrganizationKey, PercentageOfOwnership, OrganizationName, CurrencyKey
		FROM OrgPath
	)
	SELECT OrganizationKey, ParentOrganizationKey, PercentageOfOwnership, OrganizationName AS [Organization Name], CurrencyKey
	      ,CASE WHEN Delimiter1 = 0 THEN OrganizationPath 
		        ELSE SUBSTRING(OrganizationPath, 1, Delimiter1 - 1) 
		   END AS Company
		  ,CASE WHEN Delimiter1 = 0 THEN ''
				WHEN Delimiter2 = 0 THEN SUBSTRING(OrganizationPath, Delimiter1 + 1, LEN(OrganizationPath) - Delimiter1 )
				ELSE SUBSTRING(OrganizationPath, Delimiter1 + 1, Delimiter2 - Delimiter1 -1)
		   END AS Region
		  ,CASE WHEN Delimiter1 = 0 OR Delimiter2 = 0 THEN ''
		        WHEN Delimiter3 = 0 THEN SUBSTRING(OrganizationPath, Delimiter2 + 1, LEN(OrganizationPath) - Delimiter2)
				ELSE SUBSTRING(OrganizationPath, Delimiter2 + 1, Delimiter3 - Delimiter2 - 1)
		   END Country
		  ,CASE WHEN Delimiter1 = 0 OR Delimiter2 = 0 OR Delimiter3 = 0 THEN ''
		        ELSE SUBSTRING(OrganizationPath, Delimiter3 + 1, LEN(OrganizationPath) - Delimiter3)
		   END Division
	FROM DelPos