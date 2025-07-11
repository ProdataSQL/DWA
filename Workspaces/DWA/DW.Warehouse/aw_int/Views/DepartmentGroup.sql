-- Auto Generated (Do not modify) 6CE16FD9F1944562989E5F6042F5D1043A1B173C762203F402DF3C5B9DEFF110




CREATE VIEW [aw_int].[DepartmentGroup]
AS
SELECT CONVERT(VARCHAR(16),HASHBYTES('MD5', p.DepartmentGroupName),2) AS DepartmentGroupKey
	, CONVERT(VARCHAR(16),HASHBYTES('MD5', p.ParentDepartmentGroupName),2) AS ParentDepartmentGroupKey
	, ISNULL(CONVERT(VARCHAR(50), p.DepartmentGroupName ), '') AS DepartmentGroupName
	, CONVERT(VARCHAR(512), p.[FileName] ) AS FileName
	, ISNULL(CONVERT(VARCHAR(36),LineageKey ),0) AS LineageKey
FROM LH.aw_stg.departmentgroup p