-- Auto Generated (Do not modify) 640D4A4FBAB8F5218744AB85808BF550E37220C2925BD8E3ECD6B491B6E76878



CREATE VIEW [aw_int].[DepartmentGroup]
AS
SELECT CONVERT(VARCHAR(16),HASHBYTES('MD5', p.DepartmentGroupName),2) AS DepartmentGroupKey
	, CONVERT(VARCHAR(16),HASHBYTES('MD5', p.ParentDepartmentGroupName),2) AS ParentDepartmentGroupKey
	, ISNULL(CONVERT(VARCHAR(50), p.DepartmentGroupName ), '') AS DepartmentGroupName
	, ISNULL(CONVERT(bigint,BINARY_CHECKSUM(p.DepartmentGroupName )),0) AS RowChecksum
	, CONVERT(VARCHAR(512), p.[FileName] ) AS FileName
	, ISNULL(CONVERT(VARCHAR(36),LineageKey ),0) AS LineageKey
FROM LH.aw_stg.departmentgroup p