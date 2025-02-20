-- Auto Generated (Do not modify) 1C87B76C169AA25EDF6158BE6476F96E5DE06828D78A56F9FB2CC1F86EAE9F3B

/* Description: AW Dimension Scenario
   Example: EXEC dwa.usp_TableLoad NULL,4,NULL
   History: 
			19/02/2025 Created
*/
CREATE VIEW [aw_int].[Scenario] AS 
SELECT ISNULL(CONVERT(VARCHAR(16),HASHBYTES('MD5', s.ScenarioName),2),'') AS ScenarioKey
,ISNULL(CONVERT(varchar(50),s.ScenarioName),'') AS ScenarioName
, CONVERT(VARCHAR(512), s.[FileName]) AS FileName
, ISNULL(CONVERT(VARCHAR(36), s.LineageKey), 0) AS  LineageKey
FROM LH.aw_stg.scenario s;