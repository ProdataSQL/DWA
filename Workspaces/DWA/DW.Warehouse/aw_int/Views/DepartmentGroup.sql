-- Auto Generated (Do not modify) 541D423C7AC8F46A8577083779FC1B5A15B2E2936F84EF959430DE8F7A342B25


/* Description: AW DimDepartmentGroup
   Example: EXEC dwa.usp_TableLoad @TableID = 3
   History: 
			19/02/2025 Created
*/

CREATE   VIEW [aw_int].[DepartmentGroup]
AS
SELECT CONVERT(uniqueidentifier,HASHBYTES('MD5', p.ParentDepartmentGroupName),2) AS ParentDepartmentGroupKey
	, ISNULL(CONVERT(VARCHAR(50), p.DepartmentGroupName ), '') AS DepartmentGroupName
	, CONVERT(VARCHAR(512), p.[FileName] ) AS FileName
	, ISNULL(CONVERT(VARCHAR(36),LineageKey ),0) AS LineageKey
FROM LH.aw_stg.DepartmentGroup p