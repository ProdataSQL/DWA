-- Auto Generated (Do not modify) AEDD8CF50C356AD90C241D33C360C178B3596E958D85FB7C17AE49659262C0B3
/* Description: AW Staging AccountRangeRules
   Example: EXEC dwa.usp_TableLoad NULL,4,NULL
   History: 
			19/02/2025 Created	
*/
CREATE     VIEW [aw_int].[AccountRangeRules] AS 
SELECT [ReportNo]
      ,[ReportSection]
      ,[FromAccountNo]
      ,[ToAccountNo]
	  ,[FileName]
      ,ISNULL(CONVERT(VARCHAR(36),LineageKey),0) AS LineageKey
  FROM LH.aw_stg.[accountrangerules]