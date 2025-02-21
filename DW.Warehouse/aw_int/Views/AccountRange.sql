-- Auto Generated (Do not modify) EB08FB7937C620BB5B20F7C40807B1BA836CF0F66CE7588C4F83FE732E9C845A


/* Description: AW Staging AccountRange
   Example: EXEC dwa.usp_TableLoad NULL,4,NULL
   History: 
			20/02/2025 Created
*/
CREATE  VIEW [aw_int].[AccountRange] AS 
SELECT [ReportNo]
      ,[ReportHeading]
      ,[ReportHeadingNo]
      ,[ReportSection]
      ,[LineNo]
      ,[LinkLineNo]
      ,[LinkSignage]
      ,[LinkHeading]
      ,[LinkReport]
      ,[Operator]
      ,[Report]
	  ,[Calc_] as Calc1
	  ,[FileName]
	  ,ISNULL(CONVERT(VARCHAR(36),LineageKey),0) AS LineageKey
  FROM LH.aw_stg.[accountrange]