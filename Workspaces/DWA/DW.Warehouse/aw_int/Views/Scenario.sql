-- Auto Generated (Do not modify) FCFC8CA1FCD205D0A41EB1B15F3D7858CE2A71F3EB663B5F092FBA82D5620872


/* Description: AW Dimension Scenario
   Example: EXEC dwa.usp_TableLoad NULL,4,NULL
   History: 
			19/02/2025 Created
*/
CREATE   VIEW [aw_int].[Scenario] AS 
SELECT ISNULL(CONVERT(varchar(50),s.ScenarioName),'') AS ScenarioName
, CONVERT(VARCHAR(512), s.[FileName]) AS FileName
, ISNULL(CONVERT(VARCHAR(36), s.LineageKey), 0) AS  LineageKey
FROM LH.aw_stg.Scenario s;