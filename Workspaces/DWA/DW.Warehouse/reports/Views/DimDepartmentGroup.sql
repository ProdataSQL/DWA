-- Auto Generated (Do not modify) A71F75AABF4A8511B842E551A008A8C60FB84054DB5AE22412D8DA94DEDF8CB4



/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[DimDepartmentGroup]
AS
SELECT DepartmentGroupKey
      ,ParentDepartmentGroupKey
	  ,DepartmentGroupName AS [Department Name]
	  ,CASE WHEN CHARINDEX('|', DepartmentGroupPath) = 0 THEN DepartmentGroupPath ELSE SUBSTRING(DepartmentGroupPath, 1, CHARINDEX('|', DepartmentGroupPath) -1) END AS [Department Group]
      ,CASE WHEN CHARINDEX('|', DepartmentGroupPath) = 0 THEN '' ELSE SUBSTRING(DepartmentGroupPath, CHARINDEX('|', DepartmentGroupPath) + 1, LEN(DepartmentGroupPath) - CHARINDEX('|', DepartmentGroupPath) + 1) END AS Department
FROM (
	SELECT
	    TRIM('|' FROM COALESCE(d2.DepartmentGroupName, '') + '|' + COALESCE(d1.DepartmentGroupName, '')) AS DepartmentGroupPath,
		d1.*
	FROM    aw.DimDepartmentGroup d1
	LEFT JOIN
	    aw.DimDepartmentGroup d2 ON d1.ParentDepartmentGroupKey = d2.DepartmentGroupKey
) d