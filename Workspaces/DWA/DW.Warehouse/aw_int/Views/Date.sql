-- Auto Generated (Do not modify) 1FBD0A69F7FD430A9FD834BAC5C4E8DDB6006B8F01F70EDA41D10DDCD3B4BC51

/****** Object:  View [aw_int].[Date]    Script Date: 06/11/2024 19:37:48 ******/
/*  Source:       Calculated   
    Unit Test:    dwa.usp_TableLoad @TableID=1  
    History:      19/02/2025 Created    
*/
CREATE VIEW [aw_int].[Date]
AS 
SELECT ISNULL(CONVERT(VARCHAR(8),CONVERT(DATE,CONVERT(VARCHAR(10),Date),103),112),0) AS DateKey
	  ,ISNULL(CONVERT(DATE,CONVERT(VARCHAR(10),Date),103),'') AS FullDateAlternateKey
	  ,ISNULL(CONVERT(smallint,DayNumberOfWeek),0) AS DayNumberOfWeek
	  ,ISNULL(CONVERT(varchar(50),EnglishDayNameOfWeek),'') AS DayOfWeek
	  ,ISNULL(CONVERT(smallint,DayNumberOfMonth),0) AS DayNumberOfMonth
	  ,ISNULL(CONVERT(int,DayNumberOfYear),0) AS DayNumberOfYear
	  ,ISNULL(CONVERT(int,WeekNumberOfYear),0) AS WeekNumberOfYear
	  ,ISNULL(CONVERT(varchar(50),EnglishMonthName),'') AS MonthName
	  ,ISNULL(CONVERT(smallint,MonthNumberOfYear),0) AS MonthNumberOfYear
	  ,ISNULL(CONVERT(smallint,CalendarQuarter),0) AS CalendarQuarter
	  ,ISNULL(CONVERT(smallint,CalendarYear),0) AS CalendarYear
	  ,ISNULL(CONVERT(smallint,CalendarSemester),0) AS CalendarSemester
	  ,ISNULL(CONVERT(smallint,FiscalQuarter),0) AS FiscalQuarter
	  ,ISNULL(CONVERT(smallint,FiscalYear),0) AS FiscalYear
	  ,ISNULL(CONVERT(smallint,FiscalSemester),0) AS FiscalSemester
	  ,CONVERT(VARCHAR(512), [FileName]) AS FileName
	  ,ISNULL(CONVERT(VARCHAR(36),LineageKey),0) AS LineageKey
FROM [LH].[aw_stg].[date];