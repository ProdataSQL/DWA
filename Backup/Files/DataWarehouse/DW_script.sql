CREATE SCHEMA [aw]
GO

CREATE SCHEMA [aw_int]
GO

CREATE SCHEMA [dwa]
GO

CREATE SCHEMA [int]
GO

CREATE SCHEMA [reports]
GO

CREATE TABLE aw.DimAccount (AccountKey VARCHAR(16) NULL, ParentAccountKey VARCHAR(16) NULL, AccountCodeAlternateKey INT NOT NULL, ParentAccountCodeAlternateKey INT NULL, AccountDescription VARCHAR(50) NOT NULL, AccountType VARCHAR(50) NULL, Operator VARCHAR(50) NOT NULL, CustomMembers VARCHAR(50) NULL, ValueType VARCHAR(50) NOT NULL, CustomMemberOptions VARCHAR(200) NULL, RowChecksum INT NOT NULL, FileName VARCHAR(512) NULL, LineageKey VARCHAR(36) NOT NULL)    
GO


CREATE TABLE aw.DimCurrency (CurrencyKey VARCHAR(16) NULL, CurrencyAlternateKey CHAR(3) NOT NULL, CurrencyName VARCHAR(50) NOT NULL, RowChecksum BIGINT NOT NULL, FileName VARCHAR(512) NULL, LineageKey VARCHAR(36) NOT NULL)    
GO


CREATE TABLE aw.DimDate (DateKey VARCHAR(8) NOT NULL, FullDateAlternateKey DATE NOT NULL, DayNumberOfWeek SMALLINT NOT NULL, DayOfWeek VARCHAR(50) NOT NULL, DayNumberOfMonth SMALLINT NOT NULL, DayNumberOfYear INT NOT NULL, WeekNumberOfYear INT NOT NULL, MonthName VARCHAR(50) NOT NULL, MonthNumberOfYear SMALLINT NOT NULL, CalendarQuarter SMALLINT NOT NULL, CalendarYear SMALLINT NOT NULL, CalendarSemester SMALLINT NOT NULL, FiscalQuarter SMALLINT NOT NULL, FiscalYear SMALLINT NOT NULL, FiscalSemester SMALLINT NOT NULL, FileName VARCHAR(512) NULL, LineageKey VARCHAR(36) NOT NULL)    
GO


CREATE TABLE aw.DimDepartmentGroup (DepartmentGroupKey VARCHAR(16) NULL, ParentDepartmentGroupKey VARCHAR(16) NULL, DepartmentGroupName VARCHAR(50) NOT NULL, RowChecksum BIGINT NOT NULL, FileName VARCHAR(512) NULL, LineageKey VARCHAR(36) NOT NULL)    
GO


CREATE TABLE aw.DimOrganization (OrganizationKey VARCHAR(16) NOT NULL, ParentOrganizationKey VARCHAR(16) NULL, PercentageOfOwnership VARCHAR(10) NOT NULL, OrganizationName VARCHAR(50) NOT NULL, CurrencyKey VARCHAR(16) NOT NULL, RowChecksum BIGINT NOT NULL, FileName VARCHAR(512) NULL, LineageKey VARCHAR(36) NOT NULL)    
GO


CREATE TABLE aw.FactFinance (DateKey VARCHAR(8) NOT NULL, DepartmentGroupKey VARCHAR(16) NULL, ScenarioKey VARCHAR(16) NOT NULL, OrganizationKey VARCHAR(16) NOT NULL, AccountKey VARCHAR(16) NULL, Date VARCHAR(8000) NULL, Amount DECIMAL(38,6) NULL, FileName VARCHAR(512) NULL, LineageKey VARCHAR(36) NOT NULL, RowChecksum BIGINT NOT NULL)    
GO


CREATE TABLE aw.ReportAccountMap (ReportNo BIGINT NULL, Report VARCHAR(8000) NULL, AccountKey VARCHAR(16) NULL)    
GO

/****** Object:  Table [aw].[DimScenario]    Script Date: 10/03/2025 19:28:28 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [aw].[DimScenario](
	[ScenarioKey] [varchar](16) NOT NULL,
	[ScenarioName] [varchar](50) NOT NULL,
	[FileName] [varchar](512) NULL,
	[LineageKey] [varchar](36) NOT NULL
) ON [PRIMARY]
GO






/****** Object:  View [aw_int].[Currency]    Script Date: 06/11/2024 19:39:49 ******/

/* Description: AW DimCurrency
   Example: EXEC dwa.usp_TableLoad NULL,2,NULL
   History: 
			19/02/2025 Deepak Created
*/
CREATE VIEW [aw_int].[Currency]
AS
SELECT  CONVERT(VARCHAR(16),HASHBYTES('MD5', [CurrencyCode]),2) AS CurrencyKey
	, ISNULL(CONVERT(CHAR(3), [CurrencyCode]), '') AS CurrencyAlternateKey
	, ISNULL(CONVERT(VARCHAR(50), [CurrencyName]), '') AS CurrencyName
	, ROW_NUMBER() OVER (PARTITION BY   [CurrencyCode],[CurrencyName] ORDER BY [CurrencyCode] DESC) AS RowVersionNo
	, ISNULL(CONVERT(bigint,BINARY_CHECKSUM([CurrencyName],[CurrencyCode])),0) AS RowChecksum
	, CONVERT(VARCHAR(512), [FileName]) AS FileName
	,ISNULL(CONVERT(VARCHAR(36),LineageKey),0) AS LineageKey
FROM LH.aw_stg.[currency];
GO

/****** Object:  View [aw_int].[Account]    Script Date: 10/03/2025 19:11:45 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/* Description: AW Dim Account
   Example: EXEC dwa.usp_TableLoad @TableID=6
   History: 
			19/02/2025 Shruti Created
*/
CREATE VIEW [aw_int].[Account] AS
SELECT  CONVERT(VARCHAR(16),HASHBYTES('MD5', CONVERT(VARCHAR(4),a.AccountCode)),2) AS AccountKey
	, CONVERT(VARCHAR(16), c.ParentAccountKey) AS ParentAccountKey
	, ISNULL(CONVERT(INT, a.AccountCode), 0) AS AccountCodeAlternateKey
	, CONVERT(INT, a.ParentAccountCode) AS ParentAccountCodeAlternateKey
	, ISNULL(CONVERT(VARCHAR(50), a.AccountDescription COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8), '') AS AccountDescription
	, CONVERT(VARCHAR(50), a.AccountType COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8) AS AccountType
	, ISNULL(CONVERT(VARCHAR(50), a.Operator COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8), '') AS Operator
	, CONVERT(VARCHAR(50), a.CustomMembers COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8) AS CustomMembers
	, ISNULL(CONVERT(VARCHAR(50), a.ValueType COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8), '') AS ValueType
	, CONVERT(VARCHAR(200), a.CustomMemberOptions COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8) AS CustomMemberOptions
	, ISNULL(Checksum(*), 0) AS RowChecksum
	, CONVERT(VARCHAR(512), a.[FileName] COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8) AS FileName
	,ISNULL(CONVERT(VARCHAR(36),LineageKey COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8),0) AS LineageKey
FROM LH.aw_stg.[account] a
LEFT JOIN (SELECT CONVERT(VARCHAR(16),HASHBYTES('MD5', CONVERT(VARCHAR(4),ParentAccountCode)),2) AS ParentAccountKey, AccountCode  FROM LH.aw_stg.account) c	ON c.AccountCode = a.ParentAccountCode;
GO


/****** Object:  View [aw_int].[AccountRange]    Script Date: 10/03/2025 19:12:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



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
GO


/****** Object:  View [aw_int].[AccountRangeRules]    Script Date: 10/03/2025 19:12:54 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/* Description: AW Staging AccountRangeRules
   Example: EXEC dwa.usp_TableLoad NULL,4,NULL
   History: 
			19/02/2025 Created	
*/
CREATE     VIEW [aw_int].[AccountRangeRules] AS 
SELECT [ReportNo]
      ,[ReportSection] AS [ReportSection]
      ,[FromAccountNo]
      ,[ToAccountNo]
	  ,[FileName] AS FileName
      ,ISNULL(CONVERT(VARCHAR(36),LineageKey),0) AS LineageKey
  FROM LH.aw_stg.[accountrangerules]

GO


/****** Object:  View [aw_int].[Date]    Script Date: 10/03/2025 19:13:15 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


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
GO


/****** Object:  View [aw_int].[Finance]    Script Date: 10/03/2025 19:13:41 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/* Description: AW FactFinance
   Example: EXEC dwa.usp_TableLoad NULL,7,NULL
   History: 
			19/02/2025 Created
*/
CREATE VIEW [aw_int].[Finance] AS 
SELECT CONVERT(VARCHAR(8),CONVERT(DATE,CONVERT(VARCHAR(10),f.[AccountDate])),112) AS DateKey
	, f.[AccountCode] AS [AccountCodeAlternateKey]
	, f.[AccountDate] as [Date]
	, CONVERT(VARCHAR(50), f.[OrganizationName]) AS OrganizationName
	, CONVERT(VARCHAR(50), f.[DepartmentGroupName]) AS DepartmentGroupName
	, CONVERT(VARCHAR(50), f.ScenarioName) AS ScenarioName
	, SUM(CONVERT(DECIMAL(20, 6), isnull(f.[Amount], 0))) AS Amount
	, CONVERT(VARCHAR(512), f.[FileName]) AS FileName
	, ISNULL(CONVERT(VARCHAR(36), f.LineageKey), 0) AS  LineageKey
	, row_number() OVER (PARTITION BY [AccountDate], [DepartmentGroupName], [ScenarioName],[OrganizationName], [AccountCode] ORDER BY f.LineageKey DESC) AS RowVersionNo
	, ISNULL(CONVERT(BIGINT, BINARY_CHECKSUM([AccountDate], [AccountCode], [OrganizationName], [DepartmentGroupName], ScenarioName, SUM(convert(DECIMAL(16, 6), f.[Amount]) ))), 0) AS RowChecksum
FROM LH.aw_stg.transactions f
GROUP BY [AccountDate],[AccountCode],[OrganizationName],[DepartmentGroupName],ScenarioName,f.LineageKey, f.FileName
GO


/****** Object:  View [aw_int].[DepartmentGroup]    Script Date: 10/03/2025 19:13:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [aw_int].[DepartmentGroup]
AS
SELECT CONVERT(VARCHAR(16),HASHBYTES('MD5', p.DepartmentGroupName),2) AS DepartmentGroupKey
	, CONVERT(VARCHAR(16),HASHBYTES('MD5', p.ParentDepartmentGroupName),2) AS ParentDepartmentGroupKey
	, ISNULL(CONVERT(VARCHAR(50), p.DepartmentGroupName ), '') AS DepartmentGroupName
	, ISNULL(CONVERT(bigint,BINARY_CHECKSUM(p.DepartmentGroupName )),0) AS RowChecksum
	, CONVERT(VARCHAR(512), p.[FileName] ) AS FileName
	, ISNULL(CONVERT(VARCHAR(36),LineageKey ),0) AS LineageKey
FROM LH.aw_stg.departmentgroup p
GO


/****** Object:  View [aw_int].[Organization]    Script Date: 10/03/2025 19:14:11 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/* Description: AW Dimension Organization
   Example: EXEC dwa.usp_TableLoad NULL,5,NULL
   History: 
			19/02/2025 Created
*/
CREATE   VIEW [aw_int].[Organization] AS 
SELECT ISNULL(CONVERT(VARCHAR(16),HASHBYTES('MD5', p.OrganizationName),2),'') AS OrganizationKey
	, CONVERT(VARCHAR(16), c.ParentOrganizationKey) AS ParentOrganizationKey
	, ISNULL(CONVERT(VARCHAR(10), PercentageOfOwnership), 0) AS PercentageOfOwnership
	, ISNULL(CONVERT(VARCHAR(50), p.OrganizationName), '') AS OrganizationName
	, ISNULL(CONVERT(VARCHAR(16), cu.CurrencyKey), 0) AS CurrencyKey
	, ISNULL(CONVERT(bigint,BINARY_CHECKSUM(ParentOrganizationKey,p.OrganizationName, p.OrganizationName)),0) AS RowChecksum
	, CONVERT(VARCHAR(512), p.[FileName]) AS FileName
	,ISNULL(CONVERT(VARCHAR(36),p.LineageKey),0)  AS  LineageKey
FROM LH.aw_stg.organization p
LEFT JOIN (SELECT CONVERT(VARCHAR(16),HASHBYTES('MD5', OrganizationName),2) AS ParentOrganizationKey, OrganizationName FROM LH.aw_stg.organization) c 
ON c.OrganizationName=p.ParentOrganizationName
INNER JOIN aw.DimCurrency cu ON cu.CurrencyAlternateKey=p.CurrencyCode COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8;

GO


/****** Object:  View [aw_int].[Scenario]    Script Date: 10/03/2025 19:14:33 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




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
GO


/****** Object:  View [reports].[Account]    Script Date: 10/03/2025 19:14:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/* Description: AW Account for PowerBI Direct Lake Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[Account]
AS
	WITH AccPath AS
	(
		SELECT TRIM('|' FROM COALESCE(a3.AccountDescription, '') + '|' + COALESCE(a2.AccountDescription, '') + '|' + COALESCE(a1.AccountDescription, '')) AS AccountPath 
		      ,a1.AccountKey, a1.ParentAccountKey, a1.AccountCodeAlternateKey, a1.ParentAccountCodeAlternateKey, a1.AccountDescription, a1.AccountType, a1.Operator, a1.CustomMembers, a1.ValueType, a1.CustomMemberOptions
		FROM aw.DimAccount a1
		LEFT JOIN aw.DimAccount a2
			   ON a1.ParentAccountKey = a2.AccountKey
		LEFT JOIN aw.DimAccount a3
			   ON a2.ParentAccountKey = a3.AccountKey
	),
	DelPos AS 
	(
		SELECT CHARINDEX('|', AccountPath) AS Delimiter1
		      ,CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath) + 1) AS Delimiter2
			  ,CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath) + 1) + 1) AS Delimiter3
			  ,AccountPath, AccountKey, ParentAccountKey, AccountCodeAlternateKey, ParentAccountCodeAlternateKey, AccountDescription, AccountType, Operator, CustomMembers, ValueType, CustomMemberOptions
		FROM AccPath
	)
	SELECT AccountKey, ParentAccountKey
	      ,AccountCodeAlternateKey AS [Account Code]
		  ,AccountDescription AS Account
		  ,AccountType AS [Account Type], Operator
		  ,ValueType AS [Value Type]
	      ,CASE WHEN Delimiter1 = 0 THEN AccountPath 
		        ELSE SUBSTRING(AccountPath, 1, Delimiter1 - 1) 
		   END AS Report
		  ,CASE WHEN Delimiter1 = 0 THEN ''
				WHEN Delimiter2 = 0 THEN SUBSTRING(AccountPath, Delimiter1 + 1, LEN(AccountPath) - Delimiter1 )
				ELSE SUBSTRING(AccountPath, Delimiter1 + 1, Delimiter2 - Delimiter1 -1)
		   END AS [Account L2]
		  ,CASE WHEN Delimiter1 = 0 OR Delimiter2 = 0 THEN ''
		        WHEN Delimiter3 = 0 THEN SUBSTRING(AccountPath, Delimiter2 + 1, LEN(AccountPath) - Delimiter2)
				ELSE SUBSTRING(AccountPath, Delimiter2 + 1, Delimiter3 - Delimiter2 - 1)
		   END [Account L3]
	FROM DelPos

GO


/****** Object:  View [reports].[Department]    Script Date: 10/03/2025 19:15:16 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





/* Description: AW DepartmentGroup for PowerBI Direct Lake Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[Department]
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

GO


/****** Object:  View [reports].[DimAccount]    Script Date: 10/03/2025 19:15:34 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[DimAccount]
AS
	WITH AccPath AS
	(
		SELECT TRIM('|' FROM COALESCE(a3.AccountDescription, '') + '|' + COALESCE(a2.AccountDescription, '') + '|' + COALESCE(a1.AccountDescription, '')) AS AccountPath 
		      ,a1.AccountKey, a1.ParentAccountKey, a1.AccountCodeAlternateKey, a1.ParentAccountCodeAlternateKey, a1.AccountDescription, a1.AccountType, a1.Operator, a1.CustomMembers, a1.ValueType, a1.CustomMemberOptions
		FROM aw.DimAccount a1
		LEFT JOIN aw.DimAccount a2
			   ON a1.ParentAccountKey = a2.AccountKey
		LEFT JOIN aw.DimAccount a3
			   ON a2.ParentAccountKey = a3.AccountKey
	),
	DelPos AS 
	(
		SELECT CHARINDEX('|', AccountPath) AS Delimiter1
		      ,CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath) + 1) AS Delimiter2
			  ,CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath, CHARINDEX('|', AccountPath) + 1) + 1) AS Delimiter3
			  ,AccountPath, AccountKey, ParentAccountKey, AccountCodeAlternateKey, ParentAccountCodeAlternateKey, AccountDescription, AccountType, Operator, CustomMembers, ValueType, CustomMemberOptions
		FROM AccPath
	)
	SELECT AccountKey, ParentAccountKey
	      ,AccountCodeAlternateKey AS [Account Code]
		  ,AccountDescription AS Account
		  ,AccountType AS [Account Type], Operator
		  ,ValueType AS [Value Type]
	      ,CASE WHEN Delimiter1 = 0 THEN AccountPath 
		        ELSE SUBSTRING(AccountPath, 1, Delimiter1 - 1) 
		   END AS Report
		  ,CASE WHEN Delimiter1 = 0 THEN ''
				WHEN Delimiter2 = 0 THEN SUBSTRING(AccountPath, Delimiter1 + 1, LEN(AccountPath) - Delimiter1 )
				ELSE SUBSTRING(AccountPath, Delimiter1 + 1, Delimiter2 - Delimiter1 -1)
		   END AS [Account L2]
		  ,CASE WHEN Delimiter1 = 0 OR Delimiter2 = 0 THEN ''
		        WHEN Delimiter3 = 0 THEN SUBSTRING(AccountPath, Delimiter2 + 1, LEN(AccountPath) - Delimiter2)
				ELSE SUBSTRING(AccountPath, Delimiter2 + 1, Delimiter3 - Delimiter2 - 1)
		   END [Account L3]
	FROM DelPos

GO


/****** Object:  View [reports].[DimDate]    Script Date: 10/03/2025 19:15:55 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[DimDate]
AS
SELECT DateKey
	  ,FullDateAlternateKey AS Date
	  ,MonthName AS Month
	  ,FiscalQuarter AS [Fiscal Quarter]
	  ,FiscalYear AS [Fiscal Year]
	  ,MonthNumberOfYear AS [Month No]
	  ,FiscalYear * 100  + MonthNumberOfYear as [Fiscal Period No]
	  ,CONVERT (varchar(4), FiscalYear) + '-' + format(MonthNumberOfYear,'00') as [Fiscal Period] 
FROM aw.DimDate
GO


/****** Object:  View [reports].[DimDepartmentGroup]    Script Date: 10/03/2025 19:16:14 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




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

GO



/****** Object:  View [reports].[DimOrganization]    Script Date: 10/03/2025 19:16:28 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





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

GO


/****** Object:  View [reports].[DimScenario]    Script Date: 10/03/2025 19:16:41 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/* Description: AW DimScenario for PowerBI Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[DimScenario]
AS
SELECT ScenarioKey
      ,ScenarioName AS Scenario 
FROM aw.DimScenario
GO


/****** Object:  View [reports].[FactFinance]    Script Date: 10/03/2025 19:16:54 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[FactFinance]
AS
SELECT DateKey
	  ,DepartmentGroupKey
	  ,ScenarioKey
	  ,OrganizationKey
	  ,CONVERT(MONEY, Amount * CASE WHEN a.AccountType IN ('Expenditures', 'Liabilities') THEN -1 ELSE 1 END) AS BaseAmount
	  ,f.AccountKey
	  ,Date 
FROM aw.FactFinance f 
INNER JOIN aw.DimAccount a 
		ON a.AccountKey = f.AccountKey

GO


/****** Object:  View [reports].[FinancialReport]    Script Date: 10/03/2025 19:17:08 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[FinancialReport]
AS
	SELECT ReportNo AS [Report No]
	      ,ReportHeading AS [Report Heading]
		  ,ReportSection AS [Report Section]
		  ,[LineNo]
		  ,LinkLineNo
		  ,LinkReport
		  ,LinkHeading
		  
		  ,ReportHeadingNo
		  ,COALESCE(LinkSignage, 1) AS [Link Signage]
		  ,Operator
		  ,Report
		  ,Calc1
	FROM aw_int.AccountRange

GO


/****** Object:  View [reports].[General Ledger]    Script Date: 10/03/2025 19:17:23 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





/* Description: AW General Ledger for PowerBI Direct Lake Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[General Ledger]
AS
SELECT DateKey
	  ,DepartmentGroupKey
	  ,ScenarioKey
	  ,OrganizationKey
	  ,CONVERT(MONEY, Amount * CASE WHEN a.AccountType IN ('Expenditures', 'Liabilities') THEN -1 ELSE 1 END) AS BaseAmount
	  ,f.AccountKey
	  ,Date 
FROM aw.FactFinance f 
INNER JOIN aw.DimAccount a 
		ON a.AccountKey = f.AccountKey

GO


/****** Object:  View [reports].[ReportAccountMap]    Script Date: 10/03/2025 19:17:38 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[ReportAccountMap]
AS
	SELECT ReportNo, Report, AccountKey
	FROM aw.ReportAccountMap

GO




/****** Object:  StoredProcedure [aw].[usp_AccountRangeMapCreate]    Script Date: 10/03/2025 19:18:05 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*
Description:	Generate ReportAccountMap table for Financial Modelling
Used By:		Table load for ReportAccountMap

-- Example
	exec [aw].[usp_AccountRangeMapCreate]
	SELECT * FROM aw.ReportAccountMap

History:	
	20/02/2025 Created
*/

CREATE PROC [aw].[usp_AccountRangeMapCreate] AS
BEGIN
	SET NOCOUNT ON;	
	
	IF OBJECT_ID('aw.ReportAccountMap') IS NOT NULL 
		DROP TABLE aw.ReportAccountMap;

	SELECT ar.ReportNo
	, ar.Report COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8 AS Report
	, a.AccountKey COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8 AS AccountKey
	INTO aw.ReportAccountMap
	FROM aw.DimAccount a
	INNER JOIN aw_int.AccountRangeRules arr 
			ON a.AccountCodeAlternateKey BETWEEN arr.FromAccountNo AND arr.ToAccountNo
	INNER JOIN aw_int.AccountRange ar 
			ON ar.ReportNo = arr.ReportNo
	WHERE ar.Operator = 'Sum'
	
	INSERT INTO aw.ReportAccountMap (ReportNo, Report, AccountKey)
	SELECT ar.ReportNo, ar.Report, m.AccountKey
	FROM aw_int.AccountRange ar
	INNER JOIN aw_int.AccountRange ar2
			ON ar2.[LineNo] BETWEEN 1 AND ar.[LineNo] AND ar.Report = ar2.Report
	INNER JOIN aw.ReportAccountMap m 
			ON ar2.ReportNo = m.ReportNo
	WHERE ar.Operator = 'Running Sum' AND ar2.Operator = 'Sum'
	
	INSERT INTO aw.ReportAccountMap (ReportNo, Report, AccountKey)
	SELECT ar2.ReportNo, ar2.Report, m.AccountKey
	FROM aw.ReportAccountMap m
	INNER JOIN aw_int.AccountRange ar
			ON m.ReportNo = ar.ReportNo
	INNER JOIN aw_int.AccountRange ar2
			ON ar.Calc1 = ar2.ReportHeading
	WHERE ar.Operator = 'Sum' AND ar.Calc1 IS NOT NULL

END	 
	  
GO


/****** Object:  StoredProcedure [dwa].[usp_PipelinePostExecute]    Script Date: 10/03/2025 19:19:04 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/*
Description:	Log Pipline Post Execute Worker
Used By:		ADF Pipeline-Worker 
  EXEC [dwa].[usp_PipelinePostExecute] 'AB839FF1-D84D-4D78-AA11-56108971B03E' ,2
History:	
	20/02/2025  Created
*/
CREATE PROC [dwa].[usp_PipelinePostExecute] @RunID [char](36), @PipelineID [int] AS
BEGIN
	SET NOCOUNT ON;	
	DECLARE @PostExecuteSQL   VARCHAR(4000)
		   ,@SourceDirectory  VARCHAR(512)
		   ,@TargetTableName  VARCHAR(128)
		   ,@TargetDirectory  VARCHAR(512)
		   ,@ArchiveDirectory VARCHAR(4000)
		   ,@Filename NVARCHAR(200)
		   ,@LineageKey INT
		   ,@SQL NVARCHAR(max)
	
	--Archive
	SELECT  @LineageKey = l.LineageKey, @TargetTableName =p.TargetSchemaName + '.' + p.TargetTableName, @SourceDirectory= p.SourceDirectory, @ArchiveDirectory=p.ArchiveDirectory 
	FROM audit.PipelineLog p
	INNER JOIN audit.LineageLog l ON p.RunID = l.RunID AND p.SourceObject = l.SourceObject
	WHERE p.Stage = 'Extract' AND p.RunID = @RunID AND (p.PipelineID=@PipelineID OR @PipelineID=-1 OR @PipelineID IS NULL)
	GROUP BY p.PipelineID, p.RunID, l.LineageKey, p.TargetSchemaName, p.TargetTableName, p.SourceDirectory, p.ArchiveDirectory	

	IF @ArchiveDirectory IS NOT NULL 
	BEGIN
		BEGIN TRY
			SET @SQL = 'SELECT @Filename= FileName FROM FabricLH.' + @TargetTableName +' GROUP BY FileName'	
			EXEC sp_executesql @SQL, N'@Filename varchar(max) output', @Filename OUTPUT	
		END TRY
		BEGIN CATCH			
			SET @Filename = COALESCE(@Filename,NULL)
		END CATCH

		IF @Filename IS NOT NULL 
			INSERT INTO dwa.ArchiveQueue (PipelineID,	RunID,	LineageKey,	TargetTableName,	SourceDirectory,	ArchiveDirectory,Filename)
			VALUES ( @PipelineID, @RunID,	@LineageKey,	@TargetTableName, @SourceDirectory, @ArchiveDirectory,	@Filename);	
	END

	--PostExecute
	SELECT @PostExecuteSQL = p.PostExecuteSQL  
	FROM audit.PipelineLog p	
	WHERE p.PipelineID = @PipelineID AND p.RunID = @RunID
	GROUP BY p.PostExecuteSQL  
	
	IF @PostExecuteSQL IS NOT NULL 
	BEGIN
		PRINT ('/*Executing Post Execute*/' + char(13) +  @PostExecuteSQL)
		EXEC(@PostExecuteSQL )
	END	
END	 	  
GO


/****** Object:  StoredProcedure [dwa].[usp_TableCreate]    Script Date: 10/03/2025 19:19:17 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*
Description: Create a TABLE with CREATE TABLE SYNTAX. We often need to do this instead of CTAS due to IDENTITY
Example:
		[dwa].[usp_TableCreate]  1	/* Dim  */
		[dwa].[usp_TableCreate] 7 /* Fact */
History:
		20/02/2025 Created
*/
CREATE   PROC [dwa].[usp_TableCreate] @TableID [int],@TargetObject [varchar](512) =NULL AS
BEGIN	
	SET NOCOUNT ON
	BEGIN TRY
	DECLARE @ExecutionKey INT 
		, @TargetSchema sysname
		, @TargetTable sysname
		, @TableType varchar(10)
		, @ColumnStore bit
		, @SourceType varchar(10)
		, @Identity bit 
		, @sql nvarchar(4000)
		, @TargetColumns nvarchar(max)
		, @BusinessKeys nvarchar(4000)
		, @PrimaryKey sysname
		, @SourceColumns nvarchar(4000)
		, @SourceObject sysname
		, @Columns nvarchar(4000)
		, @RelatedBusinessKeys nvarchar(4000)

	IF @TableID IS NULL
		SELECT @TableID = t.TableID FROM Meta.config.edwTables t WHERE ((t.SchemaName + '.' +  t.TableName =@TargetObject) OR t.TableName =@TargetObject)

	SELECT @TargetSchema =t.SchemaName, @TargetTable =t.TableName, @TableType =t.TableType , @SourceObject =t.SourceObject, @PrimaryKey=t.PrimaryKey, @BusinessKeys =t.BusinessKeys
	FROM Meta.config.edwTables t WHERE (t.SchemaName + '.' +  t.TableName =@TargetObject) OR t.TableName =@TargetObject OR t.TableID = @TableID

	IF @TargetTable is not null SET @TargetObject=quotename(coalesce(@TargetSchema,'dbo')) + '.' + quotename(@TargetTable)
	IF @TargetTable is null
	BEGIN
		SET @TargetObject=coalesce(@TargetObject, convert(varchar(10),@TableID))
		RAISERROR ('No meta data found in edwTables for %s',16,1,@TargetObject)
	END

	IF  OBJECT_ID(@TargetObject) is NULL 
	BEGIN
		SELECT  @PrimaryKey = coalesce(@PrimaryKey, case when charindex (',',@BusinessKeys) =0 then @BusinessKeys end    ,replace(@TargetTable, 'Dim','') + 'Key')		
		SELECT @SourceColumns =string_agg(c.name, ',') FROM sys.columns c WHERE c.object_id = object_id(@SourceObject)

		SET @SQL = 'CREATE TABLE ' + @TargetObject + '(' +  char(13) 
		exec [dwa].[usp_TableCreateColumns]  @TableID, @Columns output
		SET @sql=@sql + @Columns	+ char(13) +  ');'	
		PRINT @sql 
		exec (@sql)
		SET @sql = NULL

		/*ALTER command to add Constraint*/
		IF @TableType='Dim' 
			SET @sql = 'ALTER TABLE ' +@TargetObject+' ADD CONSTRAINT [PK_' + @TargetTable + '] PRIMARY KEY NONCLUSTERED (' + @PrimaryKey + ' ASC) NOT ENFORCED;'

		PRINT @sql 
		exec (@sql)
	END
	
	/* Create PK and Indexes if Missing*/
	IF @TableType='Dim' 
	BEGIN
		IF NOT EXISTS (SELECT * From sys.indexes i where object_id=object_id(@TargetObject)  and i.name ='UX_' + @TargetTable)
		BEGIN
			IF charindex(@BusinessKeys,',') > 0 
			BEGIN
				SET @sql ='CREATE INDEX [UX_' + @TargetTable + '] ON ' + @TargetObject + '('
				SET @sql=@sql + @PrimaryKey
				SET @sql=@sql + ' )'
				PRINT @sql
				exec (@sql)
			END
		END
		
	END
	*/

	END TRY 
	BEGIN CATCH
		THROW
	END CATCH 
END
GO


/****** Object:  StoredProcedure [dwa].[usp_TableCreateColumns]    Script Date: 10/03/2025 19:19:52 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/* Description: Return the Column Meta Data DDL for a CREATE TABLE, CTAS, Polybase or other method in SQLDB or SQLDW 
Example:
		DECLARE  @Columns nvarchar(max)
		exec [dwa].[usp_TableCreateColumns]  1, @Columns output	/* Dim  */
		PRINT @Columns

		DECLARE  @Columns nvarchar(max)
		exec [dwa].[usp_TableCreateColumns] 7,@Columns output  /* Fact */
		PRINT @Columns

		DECLARE  @Columns nvarchar(max)
		exec [dwa].[usp_TableCreateColumns]  4,@Columns output  /* Dim */
		PRINT @Columns
History:
		20/02/2025  Created
*/
CREATE   PROC [dwa].[usp_TableCreateColumns] @TableID [int],@Columns [varchar](4000) OUT AS
BEGIN
	
	--SET XACT_ABORT ON
	SET NOCOUNT ON;

	BEGIN TRY
	DECLARE	@SourceObject sysname
		, @RelatedBK nvarchar(4000)
		, @BusinessKeys nvarchar(4000)
		, @PrimaryKey  sysname
		, @TableType sysname
		, @ColumnsPK nvarchar(4000)
		, @DeDupeRows int             /* Flag to check if DeDupeRows is ON for TableID */
        , @HideColumns nvarchar(4000) /* Array of Columns to Hide */
		, @PKs VARCHAR(400)
		, @BaseDims VARCHAR(400)

	SELECT @SourceObject =SourceObject, @BusinessKeys=BusinessKeys, @TableType=TableType, @PrimaryKey=PrimaryKey,@DeDupeRows = DedupeRows
	FROM Meta.config.edwTables 
	WHERE TableID=@TableID
	
	SELECT @RelatedBK = string_agg(r.BusinessKeys, ',')  
	FROM Meta.config.edwTableJoins j  
	INNER JOIN Meta.config.edwTables r on j.RelatedTableID =r.TableID
	WHERE j.TableID=@TableID

	IF @DeDupeRows=1
       SET @HideColumns =coalesce(@HideColumns + ',','')  + 'RowVersionNo';

	/* Primary Keys (if any) from joined table if Fact or Star Schema) */
	SELECT @PKs = string_agg(r.PrimaryKey, ','), @BaseDims = string_agg(r.SchemaName + '.' + r.TableName, ',')
	FROM Meta.config.edwTableJoins j
	INNER JOIN Meta.config.edwTables r ON j.RelatedTableID = r.TableID
	WHERE j.TableID = @TableID

	SET @ColumnsPK = (SELECT string_agg( c.name + ' ' + d.name  + case when c.collation_name is not null then '(' + convert (varchar(50),COALESCE(c.max_length/(length/prec),c.max_length) ) + ')'  else '' end    			+ CASE WHEN c.is_nullable=0 then ' NOT NULL' ELSE ' NULL' END 
			, char(13)  + char(9) + ','  ) WITHIN GROUP ( ORDER BY column_id) 
	FROM sys.schemas s
	INNER JOIN sys.tables t ON s.schema_id = t.schema_id
	INNER JOIN sys.columns c ON t.object_id = c.object_id
	INNER JOIN sys.types d ON d.user_type_id = c.user_type_id
	LEFT  JOIN sys.systypes stt ON stt.name = t.name AND stt.name IN ('varchar', 'nvarchar', 'nchar')
	WHERE s.name <> 'sys'
		AND s.name + '.' + t.name IN ( SELECT ltrim(value) FROM string_split(@BaseDims,','))
		AND c.name IN (SELECT ltrim(value) FROM string_split(@PKs,','))
	)
	
	IF @ColumnsPK is NOT NULL 
		SET @Columns = coalesce(@Columns + char(13) + char(9)+ ','  , char(9) )  + @ColumnsPK
	
	SET @columns = coalesce(@columns + CHAR(13) + CHAR(9) + ',', CHAR(9)) + (
		SELECT TOP 255 string_agg(c.name + ' ' + t.name 
            + CASE WHEN c.collation_name IS NOT NULL THEN '(' + CONVERT(VARCHAR(50), coalesce(c.max_length / (length / prec), c.max_length)) + ')' 
                   WHEN t.name = 'decimal' THEN '(' + CONVERT(VARCHAR(10), c.precision) + ',' + CONVERT(VARCHAR(10), c.scale) + ')' 
                   ELSE '' 
               END 
            + CASE WHEN bk.columnname IS NOT NULL OR c.is_nullable = 0 THEN ' NOT NULL' 
                   ELSE ' NULL' 
              END, CHAR(13) + CHAR(9) + ',') WITHIN	GROUP (ORDER BY column_id)
		FROM sys.columns c
		LEFT JOIN (SELECT ltrim(value) AS columnname FROM string_split(@businesskeys, ',')) bk ON c.name = bk.columnname
		INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
		LEFT JOIN sys.systypes stt ON stt.name = t.name AND stt.name IN ('varchar', 'nvarchar', 'nchar')
		WHERE c.object_id = object_id(@sourceobject) 
        AND c.name NOT IN (SELECT ltrim(value)	FROM string_split(@relatedbk  , ',')) 
        AND c.name NOT IN (SELECT ltrim(value)	FROM string_split(@hidecolumns, ','))
		)
	END TRY 
	BEGIN CATCH
		THROW
	END CATCH 
END
GO


/****** Object:  StoredProcedure [dwa].[usp_TableDrop]    Script Date: 10/03/2025 19:20:08 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/*	Drop a Star Schema Table
	Example:[dwa].[usp_TableDrop] 'aw.DimDate'
	History:
			20/02/2025 Created
*/
CREATE PROCEDURE [dwa].[usp_TableDrop] @Table [sysname]
AS
BEGIN
	DECLARE @object_id INT
	DECLARE @sql NVARCHAR(4000)
	DECLARE @is_external BIT

	SELECT @object_id = object_id, @is_external = is_external
	FROM sys.tables
	WHERE object_id = object_id(@Table)

	IF @object_id IS NOT NULL
	BEGIN
		SET @sql = 'DROP ' + CASE WHEN @is_external = 1 THEN 'EXTERNAL ' ELSE '' END + 'TABLE ' + @Table + ';'
		PRINT @sql
		EXEC (@sql)
	END
END
GO


/****** Object:  StoredProcedure [dwa].[usp_TableLoad_CheckSchemaDrift]    Script Date: 10/03/2025 19:20:28 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*
Description:	CTAS Operation for Table Load framework - Execute main usp_TableLoad proc
Example:    DECLARE @IsSchemaDrift BIT
			EXEC dwa.[usp_TableLoad_CheckSchemaDrift] 4, @IsSchemaDrift OUTPUT
			PRINT @IsSchemaDrift

			DECLARE @IsSchemaDrift BIT
			EXEC dwa.[usp_TableLoad_CheckSchemaDrift] 7, @IsSchemaDrift OUTPUT
			PRINT @IsSchemaDrift
History:	20/02/2025 Created		
*/
CREATE   PROC [dwa].[usp_TableLoad_CheckSchemaDrift] @TableID [int] , @IsSchemaDrift bit OUT AS
BEGIN
	--SET XACT_ABORT ON 	
	BEGIN 
	SET NOCOUNT ON
	DECLARE @CheckSumSource int
		  , @CheckSumTarget int
		  , @SourceObject varchar(128)
		  , @TargetObject varchar(512)
		  , @PrimaryKey   varchar(512)
		  , @BusinessKeys varchar(512)
		  , @Dimensions   varchar(512)

	SELECT  @SourceObject =t.SourceObject 
	, @TargetObject = t.SchemaName + '.' +  t.TableName
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID	

	SELECT @BusinessKeys = string_agg(r.BusinessKeys, ','),@PrimaryKey = string_agg(r.PrimaryKey, ','), @Dimensions = string_agg(r.SchemaName + '.' + r.TableName, ',')
	FROM Meta.config.edwTableJoins j
	INNER JOIN Meta.config.edwTables r ON j.RelatedTableID = r.TableID
	WHERE j.TableID = @TableID
	
	/*Check Tables with surrogate FKs*/
	IF @BusinessKeys IS NOT NULL 
		BEGIN
		SELECT @CheckSumSource = checksum(string_agg(checksum(sc.name , sc.tname ,sc.max_length ,sc.precision,sc.scale,sc.is_nullable),',')  WITHIN GROUP (Order by sc.name))
		FROM (
			SELECT c.name, st.name AS tname, c.max_length, c.precision, c.scale, c.is_nullable
			FROM sys.schemas s
			INNER JOIN sys.tables t ON s.schema_id = t.schema_id
			INNER JOIN sys.columns c ON t.object_id = c.object_id
			INNER JOIN sys.types st ON st.user_type_id = c.user_type_id
			WHERE s.name + '.' + t.name IN ( SELECT ltrim(value) FROM string_split(@Dimensions,',')) AND c.name IN (SELECT ltrim(value) FROM string_split(@PrimaryKey,','))
			UNION ALL
			SELECT  c.name , st.name as tname ,c.max_length ,c.precision,c.scale,c.is_nullable
			FROM sys.columns c			 
			INNER JOIN sys.types st ON c.user_type_id = st.user_type_id
			WHERE c.object_id = object_id(@SourceObject)  AND c.name <> 'RowVersionNo'
			AND c.name NOT IN (SELECT ltrim(value) FROM string_split(@BusinessKeys,','))
		)sc
		END
	ELSE
		BEGIN
			SELECT @CheckSumSource = checksum(string_agg(checksum(sc.name , st.name ,sc.max_length ,sc.precision,sc.scale,sc.is_nullable),',')  WITHIN GROUP (Order by sc.name))
			FROM sys.columns sc			 
			INNER JOIN sys.types st ON sc.user_type_id = st.user_type_id
			WHERE sc.object_id = object_id(@SourceObject) AND sc.name <> 'RowVersionNo';
		END

	SELECT @CheckSumTarget = checksum(string_agg(checksum(sc.name , st.name ,sc.max_length ,sc.precision,sc.scale,sc.is_nullable),',') WITHIN GROUP (Order by sc.name))
	FROM sys.columns sc			 
	INNER JOIN sys.types st ON sc.user_type_id = st.user_type_id
	WHERE sc.object_id = object_id(@TargetObject)  AND sc.name <> 'RowVersionNo';

	IF @CheckSumSource <> @CheckSumTarget
			SET @IsSchemaDrift = 1
	END
END
GO


/****** Object:  StoredProcedure [dwa].[usp_TableLoad_CTAS]    Script Date: 10/03/2025 19:20:42 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*
Description:	CTAS Operation for Table Load framework - Execute main usp_TableLoad proc
Example: exec dwa.[usp_TableLoad] 'aw.FactFinance',NULL
		 exec dwa.[usp_TableLoad] NULL, 7 
History:	20/02/2025 Created		
*/
CREATE PROC [dwa].[usp_TableLoad_CTAS] @TableID [int],@TargetObject [sysname],@SourceObject [sysname],@SelectColumns [nvarchar](max),@JoinSQL [nvarchar](max),@SqlWhere [nvarchar](max),@PrestageJoinSQL [nvarchar](max),@DeltaJoinSql [nvarchar](max),@RelatedBusinessKeys [nvarchar](max) AS
BEGIN
	--SET XACT_ABORT ON 
	
	BEGIN 
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)	      
		  , @TablePrefix nvarchar(max)		  
		  , @PrestageTargetFlag bit
		  , @CTAS bit				/* 1 if CTAS statement required */
		  , @DeleteDDL nvarchar(4000)

	SELECT @TablePrefix= coalesce(t.TablePrefix, lower(left(replace(t.TableName, 'Dim',''),1)))
	, @PrestageTargetFlag = coalesce(t.PrestageTargetFlag,0)
	, @DeleteDDL=t.DeleteDDL
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID	
	
    IF @TargetObject IS NULL
	BEGIN
		RAISERROR ('No Rule found in edwTables for %s', 16, 1, @SourceObject)
	END

	SET @sql = 'CREATE TABLE ' + @TargetObject + CHAR(13) + 'AS' + CHAR(13)
	SET @sql = @sql + 'SELECT ' + @SelectColumns + CHAR(13)
	SET @sql = @sql + 'FROM ' + @SourceObject + ' ' + @TablePrefix

	IF coalesce(@RelatedBusinessKeys, '') > ''
	BEGIN
		SET @sql = @sql + coalesce(CHAR(13) + @JoinSQL, '')
	END

	IF @PrestageJoinSQL IS NOT NULL AND @PrestageTargetFlag = 1
		SET @sql = @sql + coalesce(CHAR(13) + @PrestageJoinSQL, '')

	IF @DeltaJoinSql IS NOT NULL
		SET @sql = @sql + coalesce(CHAR(13) + @DeltaJoinSQL, '')

	IF @PrestageTargetFlag = 0 AND @DeleteDDL IS NOT NULL
		SET @SqlWhere = coalesce(@SqlWhere + CHAR(13) + CHAR(9) + ' AND ', 'WHERE ') + 'NOT (' + @TablePrefix + '.' + @DeleteDDL + ')'

	IF @sqlWhere IS NOT NULL
		SET @sql = @sql + CHAR(13) + @sqlWhere;

	SET @sql=@sql + ';' + char(13)
	PRINT @sql
	EXEC (@sql)	

	END
END
GO


/****** Object:  StoredProcedure [dwa].[usp_TableLoad_Delete]    Script Date: 10/03/2025 19:20:54 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*
Description:	DELETE Operation for Table Load framework - Execute main usp_TableLoad proc	
Example: exec dwa.[usp_TableLoad] 'aw.FactFinance',NULL
		 exec dwa.[usp_TableLoad] NULL, 7 
History:	20/02/2025  Created		
*/
CREATE   PROC [dwa].[usp_TableLoad_Delete] @TableID int,@SelectColumns nvarchar(max),@JoinSQL nvarchar(max) AS
BEGIN
	--SET XACT_ABORT ON 	
	BEGIN 
	SET NOCOUNT ON
	DECLARE @SQL nvarchar(max)
	      , @SourceObject sysname
		  , @TargetObject [sysname]
		  , @TablePrefix nvarchar(max)
		  , @BusinessKeys nvarchar(max)
		  , @DeleteDDL nvarchar(4000)

	SELECT @TargetObject = t.SchemaName + '.' + t.TableName, @TablePrefix= coalesce(t.TablePrefix, lower(left(replace(t.TableName, 'Dim',''),1)))
	, @SourceObject =t.SourceObject
	, @BusinessKeys =t.BusinessKeys
	, @DeleteDDL=t.DeleteDDL
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID	
	
    IF @TargetObject IS NULL
	BEGIN
		RAISERROR ('No Rule found in edwTables for %s', 16, 1, @SourceObject)
	END

	SET @SQL ='DELETE t ' + char(13) + 'FROM ' +  @TargetObject  + ' t' + char(13) 
	IF @JoinSQL IS NOT NULL
		BEGIN
			SET @SQL =@SQL + 'INNER JOIN  ( SELECT ' + @SelectColumns + char(13) + 'FROM ' + @SourceObject +' ' + @TablePrefix 
			SET @SQL=  coalesce(@SQL +  char(13)+  @JoinSQL ,@SQL)
			SET @SQL = @SQL + char(13)+' WHERE '+@TablePrefix +'.'+ @DeleteDDL + char(13)+ ') s ON '
			SET @SQL = @SQL + (	SELECT string_agg ('s.' + ltrim(bk.value) + '=t.' + ltrim(bk.value)  , ' AND ') 
								FROM string_split(@BusinessKeys,',') bk ) 
		END
	ELSE
		BEGIN
			SET @sql = @sql + 'INNER JOIN ' + @SourceObject + ' s ON '
			SET @sql = @sql + (	SELECT string_agg ('s.' + ltrim(bk.value) + '=t.' + ltrim(bk.value)  , ' AND ') 
								FROM string_split(@BusinessKeys,',') bk ) + char(13) + 'AND s.' + @DeleteDDL
		END	 
	PRINT @SQL + char(13)
	EXEC (@SQL)

	END
END
GO


/****** Object:  StoredProcedure [dwa].[usp_TableLoad_DeploySchemaDrift]    Script Date: 10/03/2025 19:21:06 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*
Description:	DeploySchemaDrift for Table Load framework - Execute main usp_TableLoad proc
Example:  EXEC dwa.[usp_TableLoad_DeploySchemaDrift] 4
History:	20/02/2025 Created		
*/
CREATE PROC [dwa].[usp_TableLoad_DeploySchemaDrift] @TableID [int] AS
BEGIN
	--SET XACT_ABORT ON 	
	BEGIN 
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)	      	  
		  , @SourceObject varchar(128)
		  , @TargetObject varchar(512)
		  , @TempTable varchar(102)
		  , @SelectColumns [nvarchar](max)
		  , @AddColumns [nvarchar](max)
		  , @RowChecksum [nvarchar](max)

	SELECT @SourceObject =t.SourceObject 
	, @TargetObject = t.SchemaName + '.' +  t.TableName
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID		
   
	SET @TempTable = @TargetObject+'_tmp'
	
		BEGIN
			IF OBJECT_ID(@TempTable) IS NOT NULL
				EXEC [dwa].[usp_TableDrop] @Table= @TempTable
			SET @SQL = 'CREATE TABLE ' +@TargetObject+'_tmp AS' + char(13) + 'SELECT * FROM '+@TargetObject +';'
			PRINT @SQL
			EXEC(@SQL)
			EXEC [dwa].[usp_TableDrop] @Table= @TargetObject;
			EXEC [dwa].[usp_TableCreate] @TableID = @TableID		

			SET  @AddColumns= (	
				SELECT string_agg(
						CASE WHEN c.is_nullable = 1 THEN 'NULL' 
							 WHEN c.name LIKE '%RowChecksum%' THEN '-1' 
							 WHEN st.name LIKE '%char%' AND c.is_nullable = 0 THEN '''''' 
							 WHEN st.name IN ('decimal', 'numeric', 'int') AND c.is_nullable = 0 THEN '0' 
						END + '  AS ' + c.name + ' ', ',')
				FROM sys.columns c
				INNER JOIN sys.types st ON c.user_type_id = st.user_type_id
				WHERE c.object_id = object_id(@TargetObject) AND c.name NOT IN (SELECT name FROM sys.columns WHERE object_id = object_id(@TempTable))
			)		

			SET @RowChecksum = (	
				SELECT string_agg(c.name, ',')
				FROM sys.columns c
				WHERE c.object_id = object_id(@TargetObject) 
				AND c.name IN (SELECT name FROM sys.columns WHERE object_id = object_id(@TempTable))
				AND c.name LIKE '%RowChecksum%')	

			SET @SelectColumns = (	
				SELECT string_agg(c.name, ',')
				FROM sys.columns c
				WHERE c.object_id = object_id(@TargetObject) AND c.name IN (SELECT name FROM sys.columns WHERE object_id = object_id(@TempTable))
				AND c.name NOT LIKE '%RowChecksum%')	
							
			SET @SQL = 'INSERT INTO ' + @TargetObject + char(13) + 'SELECT ' + @SelectColumns 
			IF @AddColumns  IS NOT NULL SET @SQL = @SQL + ' , ' +  @AddColumns 			
			IF @RowChecksum IS NOT NULL  SET @SQL = @SQL + ' , ' + '-1 AS' + @RowChecksum
			SET @SQL = @SQL + char(13) + 'FROM ' + @TempTable

			PRINT @SQL
			EXEC (@SQL)
			IF OBJECT_ID(@TempTable) IS NOT NULL
				EXEC [dwa].[usp_TableDrop] @Table=@TempTable
			END
	END
END
GO


/****** Object:  StoredProcedure [dwa].[usp_TableLoad_Insert]    Script Date: 10/03/2025 19:21:19 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*
Description:	INSERT Operation for Table Load framework - Execute main usp_TableLoad proc
Example: exec dwa.[usp_TableLoad] 'aw.FactFinance',NULL,NULL
		 exec dwa.[usp_TableLoad] NULL, 1,NULL
History:		20/02/2025  Created	
*/
CREATE   PROC [dwa].[usp_TableLoad_Insert] @TableID [int],@TargetObject [sysname],@SourceObject [sysname],@InsertColumns [nvarchar](max),@SelectColumns [nvarchar](max),@JoinSQL [nvarchar](max),@SqlWhere [nvarchar](max),@WhereJoinSQL [nvarchar](max),@Exists [bit] AS
BEGIN
	--SET XACT_ABORT ON 
	
	BEGIN 
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)	      
		  , @TablePrefix nvarchar(max)
		  , @BusinessKeys nvarchar(max)
		  , @PrestageTargetFlag bit
		  , @PrestageTargetObject sysname /* Full Name of Final Target Table if prestaging */

	IF @TableID IS NULL
		SELECT @TableID = t.TableID FROM Meta.config.edwTables t WHERE ((t.SchemaName + '.' +  t.TableName =@TargetObject) OR t.TableName =@TargetObject)
	SET @sqlWhere = COALESCE(@sqlWhere,'')
	SELECT @TablePrefix= coalesce(t.TablePrefix, lower(left(replace(t.TableName, 'Dim',''),1)))
	, @BusinessKeys =t.BusinessKeys
	, @PrestageTargetFlag = coalesce(t.PrestageTargetFlag,0)
	, @PrestageTargetObject =coalesce(t.PrestageSchema,'stg') + '.'+ coalesce(t.PrestageTable, t.TableName)
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID	
	
    IF @TargetObject IS NULL
	BEGIN
		RAISERROR ('No Rule found in edwTables for %s', 16, 1, @SourceObject)
	END
	IF @PrestageTargetFlag = 1
		BEGIN
			SELECT @SelectColumns =  (SELECT string_agg( c.name, ',') FROM sys.columns c WHERE c.object_id = object_id(@TargetObject) AND  c.name NOT IN('RowVersionNo', 'RowOperation' ) )
			SELECT @InsertColumns=   @SelectColumns
			SET @sqlWhere =  'WHERE RowOperation=''I'''
		END

	SET @SQL ='INSERT INTO ' + @TargetObject + ' (' + @InsertColumns + ')'	+ char(13)
	IF @BusinessKeys is not null and @Exists =1	SET @SQL =@SQL + 'SELECT '+ @TablePrefix +'.* FROM (' + char(13) 
	SET @SQL =@SQL + 'SELECT ' + @SelectColumns + char(13) + 'FROM ' + @SourceObject + ' '+  @TablePrefix   
	SET @SQL=  coalesce(@SQL +  char(13)+  @JoinSQL ,@SQL)	

	IF @BusinessKeys is not null and @Exists =1	SET @SQL =@SQL + char(13) + @sqlWhere +  ' ) ' + @TablePrefix 
	SET @JoinSQL =null
	IF @BusinessKeys is not null and @Exists =1
	BEGIN		
	    
		SELECT @JoinSQL = coalesce( NULL+  ' AND ','WHERE ' ) + 'NOT EXISTS (SELECT * FROM ' + @TargetObject + ' t WHERE '
		IF @WhereJoinSQL IS NOT NULL
			SELECT @JoinSQL = @JoinSQL + (SELECT string_agg(rtrim(bk.value), ' AND ') + ')'  FROM string_split(rtrim(@WhereJoinSQl), char(13)) bk)
		ELSE
			SELECT @JoinSQL = @JoinSQL + (
							SELECT string_agg(@TablePrefix + '.' + ltrim(bk.value) + '=t.' + ltrim(bk.value), ' AND ') + ')'
							FROM string_split(@BusinessKeys, ',') bk
							)  
		IF @JoinSQL is not null SET @sql=  @sql + char(13) + @JoinSQL
	END
	IF @JoinSQL IS NULL AND  @sqlWhere IS NOT NULL SET @sql=@sql + char(13) +  @sqlWhere         
	SET @SQL=@SQL + ';' +char(13) 
	PRINT @sql 
		EXEC (@sql)	
	END
END
GO


/****** Object:  StoredProcedure [dwa].[usp_TableLoad_PrestageView]    Script Date: 10/03/2025 19:21:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*
Description:Materialize VIEWs into _staging table
Example:	exec dwa.[usp_TableLoad_PrestageView] NULL, 2
			exec dwa.[usp_TableLoad_PrestageView] 'dwa.usp_TableLoad_PrestageView', NULL  --Invalid Example
History:	20/02/2025 Created		
*/
CREATE  PROC [dwa].[usp_TableLoad_PrestageView] @TargetObject [sysname],@TableID [int] AS
BEGIN
	--SET XACT_ABORT ON 
	BEGIN 
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)
	      , @IsView bit =0
	      , @TableName sysname 
	      , @SourceObject sysname
		  , @PrestageTargetObject sysname /* Full Name of Final Target Table if prestaging */

	SELECT @TableName =TableName,  @TargetObject = t.SchemaName + '.' + t.TableName ,@SourceObject =t.SourceObject
	, @PrestageTargetObject =coalesce(t.PrestageSchema,'stg') + '.'+ coalesce(t.PrestageTable, t.TableName)+'_staging'
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID
	

    IF @TargetObject IS NULL
	BEGIN
		RAISERROR ('No Rule found in edwTables for %s',16,1,@SourceObject)		
	END

	IF (SELECT OBJECTPROPERTY(OBJECT_ID(@SourceObject), 'IsView') AS [IsView]) = 1
		SET @IsView = 1	
	ELSE
		BEGIN
		SET @IsView = 0
		RAISERROR ('No valid view found for Targetobject: %s',16,1,@Targetobject)		
		END

	PRINT '/* --Prestaging VIEW data into staging table--*/' + char(13) + char(10) +  'exec dwa.[usp_TableLoad_PrestageView] @TargetObject=''' + @TargetObject+''',@TableID=' + convert(varchar, @TableID)+  char(10) 
	IF @IsView = 1
	BEGIN							
			IF object_id(@PrestageTargetObject) IS NOT NULL
			BEGIN
				SET @sql = 'DROP TABLE ' + @PrestageTargetObject
				PRINT @sql   
				EXEC (@sql)
			END
	
			SET @sql = 'SELECT * INTO ' +@PrestageTargetObject  + ' FROM ' +@SourceObject	
			PRINT  @sql   
			EXEC (@sql)		
	END

	END
END
GO


/****** Object:  StoredProcedure [dwa].[usp_TableLoad_Update]    Script Date: 10/03/2025 19:21:46 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*
Description:	UPDATE Operation for Table Load framework - Execute main usp_TableLoad proc
Example: exec dwa.[usp_TableLoad] 'aw.FactFinance',NULL
		 exec dwa.[usp_TableLoad] NULL, 1 ,NULL,NULL
History:	20/02/2025 Created		
*/
CREATE PROC [dwa].[usp_TableLoad_Update] @TableID [int],@TargetObject [sysname],@SourceObject [sysname],@UpdateColumns [nvarchar](max),@SelectColumns [nvarchar](max),@JoinSQL [nvarchar](max),@SqlWhere [nvarchar](max),@WhereJoinSQL [nvarchar](max),@Exists [bit] AS
BEGIN
	--SET XACT_ABORT ON 
	BEGIN 
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)	      
		  , @TablePrefix nvarchar(max)
		  , @BusinessKeys nvarchar(max)
		  , @PrestageTargetFlag bit

	SELECT  @TablePrefix= coalesce(t.TablePrefix, lower(left(replace(t.TableName, 'Dim',''),1)))
	, @BusinessKeys =t.BusinessKeys
	, @PrestageTargetFlag = t.PrestageTargetFlag
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID	
	
    IF @TargetObject IS NULL
	BEGIN
		RAISERROR ('No Rule found in edwTables for %s', 16, 1, @SourceObject)
	END
	IF @PrestageTargetFlag = 1
		BEGIN
			SELECT @SelectColumns =  '*'
			SET @sqlWhere =  'WHERE RowOperation=''U'''
		END

	SET @JoinSQL = ' '+COALESCE(@JoinSQL,'')  + ' '+ COALESCE(@SqlWhere,'') + ') '+@TablePrefix+' ON ' + char(13) 
	SET @SQL = 'UPDATE t ' + char(13) + 'SET '
	SET @SQL = @SQL + @UpdateColumns + char(13)
	SET @SQL = @SQL + 'FROM ' + @TargetObject  + ' t' + char(13) 			

	IF @JoinSQL IS NOT NULL  
	BEGIN
		SET @SQL =@SQL + 'INNER JOIN (SELECT ' + @SelectColumns  + ' FROM ' + @SourceObject +' ' + @TablePrefix 
		SET @SqlWhere = NULL
	END						
    ELSE
		SET @SQL = @SQL + ' INNER JOIN ' + @SourceObject + ' ' + @TablePrefix + ' ON ' 

	SET @SQL=  coalesce(@SQL +  @JoinSQL ,@SQL)
	SET @SQL = @SQL + (	SELECT string_agg (@TablePrefix + '.' + ltrim(bk.value) + '=t.' + ltrim(bk.value)  , ' AND ') 	FROM string_split(@BusinessKeys,',') bk ) 	
	IF COALESCE(@PrestageTargetFlag,0) = 0 AND charindex ('.RowChecksum',@UpdateColumns) > 0 
		SET @SQL = @SQL + char(13) + 'AND ' + @TablePrefix + '.RowChecksum <> t.RowChecksum'  
	IF @SqlWhere is not null SET @sql=@sql + char(13) + @SqlWhere
	SET @sql =@sql + ';'
	PRINT @sql 
	EXEC (@sql)	
	END
END
GO


/****** Object:  StoredProcedure [dwa].[usp_TableLoad]    Script Date: 10/03/2025 19:22:00 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






/*
Load Table into DWH with "smart" loading: selection of SELECT INTO v INSERT INTO ,DDL Changes, FK Maintenance
		Assumes view has correct Semantic Columns like RowcheckSum
Used By: LoadWorker ADF Pipeline
Example:
	exec [dwa].[usp_TableLoad] NULL,1,NULL
	[dwa].[usp_TableLoad] 'aw.DimDate' 
	[dwa].[usp_TableLoad] 'aw.FactFinance'

History:
	20/02/2025 Created
*/
CREATE     PROC [dwa].[usp_TableLoad] @TargetObject [nvarchar](512) =NULL, @TableID [int]= NULL, @RunID uniqueidentifier = NULL AS
BEGIN
	BEGIN TRY
	SET NOCOUNT ON 
	DECLARE  @SchemaName nvarchar(128)
		, @TableName nvarchar(128)
		, @TableType nvarchar(10)
		, @SourceType nvarchar(10)
		, @SourceObject nvarchar(128)
		, @SourceSchema nvarchar(128)
		, @SourceTable varchar(128)
		, @TargetSchema varchar(128)
		, @TargetTable varchar(128)
		, @sql nvarchar(4000)
		, @InsertColumns varchar(max)
		, @SourceColumns varchar(max)
		, @SelectColumns varchar(max)
		, @TargetColumns varchar(max)
		, @UpdateColumns varchar(max)
		, @JoinSQL nvarchar(4000)
		, @BusinessKeys varchar(4000)
		, @RelatedBusinessKeys varchar(max)
		, @PrimaryKey varchar(128)
		, @InsertCount int
		, @UpdateCount int		
		, @UpdateFlag bit		/* 1 if an Update required */
		, @TablePrefix nvarchar(128)	/* Alias for Target Table in Dynamic TSQL */
		, @AutoDrop bit			/* 1 to force a drop of Table */
		, @AutoTruncate bit			/* 1 to force Truncate Table before Load */
		, @CTAS bit				/* 1 if CTAS statement required */
		, @Exists bit			 /* 1 if data exists in TargetObject   */
		, @DedupeRows bit		 /* 1 if We Dedupe Rows by using RowVersionNo =1*/
		, @sqlWhere varchar(4000) /* Additional Where Clause predicates */
		, @HideColumns varchar(4000) /* Array of Columns to Hide */
		, @PrestageSourceFlag bit /* 1 if Prestage Source View to Staging table */
		, @PrestageTargetObject varchar(128) /* Full Name of Final Target Table if prestaging */
		, @PrestageTargetFlag bit			/* 1 if Target Table is a prestaged and needs DeltaAction 'N,X,C' to track New, Delete, Change */
		, @PrestageJoinSQL varchar(4000)	 /* Join Criteria for Prestaging */
		, @PrestageSchema varchar(128) 
		, @PrestageTable varchar(128)
		, @DeleteDDL varchar(4000)
		, @DeleteFlag bit 
		, @InsertFlag bit
		, @SkipSqlCondition varchar(4000)
		, @DeltaTargetObject varchar(128)
		, @DeltaJoinSQL varchar(4000)	 /* Join Criteria for Delta ot DeltaTargetObject */
		, @i int
		, @Skip	bit						/* 1 if Processing to Skip */
		, @PreLoadSQL	varchar(512)		/* TSQL or Proc for Infer */
		, @PostLoadSQL varchar(512)		/* TSQL or Proc for Validate */
		, @JoinType nvarchar(25)			/* Default INNER JOIN, but can be override to LEFT */
		, @Language nvarchar(128)				/* British if dates need to be DMY */
		, @WhereJoinSQL varchar(4000)   /* Variable for JOIN SQL between Fact and View for WHERE NOT EXISTS*/ 
		, @LocalJoinSQL varchar(4000)
		, @SchemaDrift bit
		, @RelatedTableCount smallint =0
		, @VarHideCols VARCHAR(4000) --Temp Variable to store hide columns value
		, @IsSchemaDrift bit
		, @IsIncremental bit
		, @ForceCollation bit = 0
		, @ColumnsCollation varchar(512) = NULL
		, @SourceCollation sysname
		, @TargetCollation sysname;

	IF @TableID IS NULL
		SELECT @TableID = t.TableID FROM Meta.config.edwTables t WHERE ((t.SchemaName + '.' +  t.TableName = @TargetObject) OR t.TableName =@TargetObject);

	SELECT @SchemaName =SchemaName, @TableName =TableName, @SourceObject =t.SourceObject  ,@TableType =t.TableType , @TargetSchema =t.SchemaName ,  @TargetTable =t.TableName,  @SourceType=t.SourceType,  @PrimaryKey=t.PrimaryKey, @BusinessKeys =t.BusinessKeys, @UpdateFlag=coalesce(t.UpdateFlag,1)
	, @TablePrefix= coalesce(t.TablePrefix , case when t.TableType='Fact' THEN 'f' ELSE lower(left(replace(t.TableName, 'Dim',''),1)) END)
	, @AutoDrop=coalesce (t.AutoDrop,0)
	, @AutoTruncate =coalesce(t.AutoTruncate,0)
	, @CTAS =t.CTAS
	, @DedupeRows = coalesce(t.DedupeRows,0)
	, @PrestageSourceFlag =coalesce(t.PrestageSourceFlag,0)
	, @PrestageTargetFlag=coalesce(t.PrestageTargetFlag,0)
	, @PrestageSchema = coalesce(t.PrestageSchema,'stg')
	, @PrestageTable =coalesce(t.PrestageTable, t.TableName)
	, @PrestageTargetObject =coalesce(t.PrestageSchema,'stg') + '.'+ coalesce(t.PrestageTable, t.TableName)
	, @DeleteDDL=t.DeleteDDL
	, @TargetObject=t.SchemaName + '.' +  t.TableName 
	, @DeleteFlag =coalesce(t.DeleteFlag, case when t.DeleteDDL is not null then 1 else 0 end )
	, @SkipSqlCondition = t.SkipSqlCondition
	, @InsertFlag =coalesce(t.InsertFlag,1)
	, @DeltaTargetObject= t.DeltaTargetObject
	, @Skip=0
	, @PostLoadSQL =t.PostLoadSQL
	, @PreLoadSQL =t.PreLoadSQL
	, @JoinType=coalesce(t.JoinType, 'INNER') 
	, @SqlWhere =coalesce('WHERE ' + t.WhereSQL,null)
	, @Language =t.[Language]
	, @SchemaDrift =case when t.InsertFlag =0 then 0 else coalesce(t.SchemaDrift,1) end 
	, @IsIncremental = coalesce(t.IsIncremental,0)
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID

	IF @@LANGUAGE <> @Language  and coalesce(@Language,'' ) <> ''
	BEGIN
		IF @Language ='British'
			SET LANGUAGE British
		ELSE IF @Language in ('us_English','English')
			SET LANGUAGE us_English
		ELSE
			raiserror ('Unsupported Language [%s]',16,1,@Language)

		SET @sql = 'SET LANGUAGE ' + @Language
		PRINT @sql
	END

	IF @PreLoadSQL is not null
	BEGIN
		PRINT '--PreLoadSQL Event '
		PRINT @PreLoadSQL
		PRINT ''
		exec (@PreLoadSQL)
	END

	IF charindex('.',@SourceObject) > 0
		BEGIN
			SET @SourceSchema = left( @SourceObject, charindex('.',@SourceObject)-1 )
			SET @SourceTable= substring( @SourceObject, charindex('.',@SourceObject)+1 , 255)
		END
	ELSE
		BEGIN
			SELECT @SourceSchema ='dbo', @SourceTable =@SourceObject 
			SET @SourceObject ='dbo.' + @SourceObject 
		END

	IF @AutoDrop=1
		SELECT @UpdateFlag =0, @DeleteFlag=0, @SchemaDrift=0
    
	IF @TableName is null
	BEGIN
		SET @TargetObject=coalesce(@TargetObject, convert(varchar(10),@TableID))
		RAISERROR ('No meta data found in edwTables for %s',16,1,@TargetObject)
	END
	SET @CTAS= CASE WHEN (object_id(@TargetObject) is null or @AutoDrop=1) AND coalesce(@CTAS,1)=1 THEN 1 ELSE 0 END 

	IF object_id(@TargetObject) IS NOT NULL AND @IsIncremental = 1
		SELECT @AutoDrop = 0, @AutoTruncate=0,@InsertFlag=1,@CTAS=0

	IF object_id(@TargetObject) IS NULL OR (@PrestageTargetFlag=1 AND @AutoDrop=0) 
		SET @UpdateFlag=0
    
   	IF @RunID IS NULL	SET @RunID = NEWID()
		
	IF @SkipSqlCondition IS NOT NULL
	BEGIN
		SET @sql = 'SELECT @i=CASE WHEN (' + @SkipSqlCondition + ') THEN 1 ELSE 0 END'
		EXEC sp_executesql @sql, N'@i int output', @i OUTPUT
		IF @i = 1
			SELECT @UpdateFlag = 0, @InsertFlag = 0, @DeleteFlag = 0, @AutoDrop = 0, @AutoTruncate = 0, @Skip = 1
		IF @Skip = 1
			PRINT '/* Skip Condition True(' + @SkipSqlCondition + '). Load Skipped */'
	END

	IF object_id(@SourceObject) IS NULL AND @Skip = 0
		RAISERROR ('SourceObject %s not found. Check Meta.config.edwTables for TableID %i', 16, 1, @SourceObject, @TableID)

	IF @DedupeRows =1 and @skip=0
	BEGIN
		
		IF not exists (Select * From sys.columns c where c.name='RowVersionNo' AND c.object_id=object_id(@SourceObject))
		raiserror ('Table %s, Source %s must contain column [RowVersionNo] if DedupeRows=1 in Meta.config.edwTables.',16,1,@TargetObject, @SourceObject)
		SET @sqlWhere= coalesce(@sqlWhere + char(13) + char(9) + ' AND ', 'WHERE ') +@TablePrefix + '.RowVersionNo=1' 
	END
	   
	IF @AutoDrop =1 AND object_id(@TargetObject) is not null 
		exec [dwa].usp_TableDrop @TargetObject
	IF @AutoTruncate =1 AND object_id(@TargetObject) is not null 
	BEGIN
		SET @sql = 'TRUNCATE TABLE ' + @TargetObject
		PRINT  @sql 
		EXEC (@sql)
	END
	
	IF object_id(@TargetObject) is null 
		SELECT @Exists=0 , @IsIncremental=0
	ELSE
		BEGIN
			IF @SourceType='View'
			BEGIN
				set @sql = 'SELECT @Exists=CASE WHEN EXISTS (SELECT * FROM ' + @TargetObject + ') THEN 1 ELSE 0 END '
				exec sp_executesql @sql, N'@Exists bit output', @Exists output
			END
			ELSE
				SET @Exists=1
		END	

	IF @SourceType ='Proc' and @Skip=0
	BEGIN
		SET @sql = 'exec ' + @SourceObject 
		PRINT  @sql   
		exec (@sql)	
	END
	ELSE IF @SourceType ='View' and @Skip=0
	BEGIN	
		IF @TableType='Dim' and Coalesce(@UpdateFlag,1) =1
			IF NOT EXISTS (SELECT * from sys.columns c WHERE object_id=object_id(@SourceObject) and c.name='RowChecksum')				
				raiserror('View ''%s'' is missing column RowChecksum, add this to View Definition. Either Checksum on non keys, or set UpdateFlag=0 if not updates in Meta.config.edwTables',16,1,@SourceObject)
		IF @TableType='Fact' and Coalesce(@UpdateFlag,1) =1
			IF NOT EXISTS (SELECT * from sys.columns c WHERE object_id=object_id(@SourceObject) and c.name='RowChecksum') 
				SET @UpdateFlag=0;
		
		/* Check if collation between LH and DW is different */
		SELECT @TargetCollation = CONVERT(sysname, DATABASEPROPERTYEX(db_name(), 'Collation'))
		SELECT TOP 1 @SourceCollation = collation_name FROM sys.columns 
			   WHERE object_id = object_id(@SourceObject) AND collation_name IS NOT NULL AND collation_name <> @TargetCollation
		SELECT @ColumnsCollation = STRING_AGG(name, ',') FROM sys.columns 
			   WHERE object_id = object_id(@SourceObject) AND collation_name IS NOT NULL AND collation_name <> @TargetCollation
		IF COALESCE(@SourceCollation, @TargetCollation) <> @TargetCollation
			SET @ForceCollation = 1

		IF @DeDupeRows=1
			SET @HideColumns =coalesce(@HideColumns + ',','')  + 'RowVersionNo';

		;WITH s AS(
			SELECT value as colname FROM string_split(@ColumnsCollation, ',')
		), r AS (
			SELECT r.TableID AS RelatedTableID
				, coalesce(j.JoinSQL, string_agg(@TablePrefix + '.' + ltrim(bk.value) + CASE WHEN @ForceCollation = 1 AND s.colname IS NOT NULL THEN ' COLLATE ' + @TargetCollation  ELSE '' END + ' = ' + coalesce(r.TablePrefix, lower(left(replace(r.TableName, 'Dim', ''), 1))) + '.' + ltrim(bk.value), ' AND ')) fk
				, r.BusinessKeys, r.SchemaName, r.TableName, r.PrimaryKey
				, min(coalesce(r.TablePrefix, lower(left(replace(r.TableName, 'Dim', ''), 1)))) TablePrefix
				, min(coalesce(j.JoinSQL, 't.' + ltrim(r.PrimaryKey) + '=' + coalesce(r.TablePrefix, lower(left(replace(r.TableName, 'Dim', ''), 1))) + '.' + ltrim(r.PrimaryKey))) WhereJoinSQL
				, coalesce(j.JoinType, @JoinType) AS JoinType
			FROM Meta.config.edwTables r
			INNER JOIN Meta.config.edwTableJoins j ON j.RelatedTableID = r.TableID
			CROSS APPLY string_split(r.BusinessKeys, ',') bk
			LEFT JOIN s ON s.colname = bk.value --To check for columns that needed collation
			WHERE j.TableID = @TableID
			GROUP BY r.TableID, r.BusinessKeys  , r.SchemaName, r.TableName, r.PrimaryKey, j.JoinSQL, j.JoinType
		), t AS (
			SELECT t.TableID, coalesce(t.TablePrefix, lower(left(replace(t.TableName, 'Dim', ''), 1))) AS TablePrefix
			FROM Meta.config.edwTables t
			WHERE t.TableID = @TableID
		)
		
		SELECT @InsertColumns = string_agg( coalesce(j.AliasPK, r.PrimaryKey), ',')
		, @SelectColumns=string_agg( r.TablePrefix + '.' + r.PrimaryKey  + coalesce(' AS ' + j.AliasPK,'') , ',') --+ char(13)
		, @RelatedBusinessKeys = string_agg( r.BusinessKeys, ',') 
		, @JoinSQL = string_agg(  r.JoinType+ ' JOIN ' + quotename(r.SchemaName) + '.' + quotename (r.TableName) + ' ' + r.TablePrefix	+ ' ON ' +  r.fk  ,char(13)) WITHIN GROUP (Order by j.JoinOrder)
		, @LocalJoinSQL = string_agg(r.WhereJoinSQL +' ' ,char(13))
		, @RelatedTableCount=count(*)
		FROM Meta.config.edwTableJoins  j 
		INNER JOIN r on j.RelatedTableID =r.RelatedTableID
		WHERE j.TableID=@TableID
		
		IF @BusinessKeys IS NOT NULL 
			SET @WhereJoinSQL=null
		ELSE
			SET @WhereJoinSQL=coalesce(@WhereJoinSQL + ' AND ' + @LocalJoinSQL,@LocalJoinSQL)
		
		/* Check Row Does not exist in another target */
		IF @DeltaTargetObject is not null and object_id(@DeltaTargetObject) is not null and @BusinessKeys is not null AND OBJECT_ID(@TargetObject) is null
		BEGIN
			SET @DeltaJoinSql ='LEFT JOIN ' + @DeltaTargetObject+ ' t ON '
			IF @LocalJoinSQL IS NULL
				SET @DeltaJoinSql = @DeltaJoinSql +  (SELECT string_agg( @TablePrefix + '.'+ ltrim([value]) +'=' + 't.' + ltrim([value]) ,' AND ')   FROM (SELECT value from string_split(@BusinessKeys,',') ) a )		
			ELSE 
				SET @DeltaJoinSql = @DeltaJoinSql + (SELECT string_agg(rtrim(bk.value), ' AND ')  FROM string_split(rtrim(@LocalJoinSQL), ' ') bk)
			SET @sqlWhere= coalesce(@sqlWhere + char(13) + char(9) + ' AND ', 'WHERE ')  + '('+@TablePrefix +'.RowChecksum <> t.RowChecksum OR t.RowChecksum is null)'
		END
		ELSE IF @DeltaTargetObject is not null
			SET @sqlWhere=null

		/* SchemaDrift*/
		IF @SchemaDrift=1 AND COALESCE(@AutoDrop,0)=0 and  object_id(@TargetObject) IS NOT NULL
		BEGIN
			 EXEC [dwa].[usp_TableLoad_CheckSchemaDrift] @TableID=@TableID,@IsSchemaDrift = @IsSchemaDrift OUTPUT		
			 IF @IsSchemaDrift = 1
				EXEC [dwa].[usp_TableLoad_DeploySchemaDrift] @TableID = @TableID
		END	

		IF @InsertColumns IS NOT NULL
		SET @HideColumns = coalesce(@HideColumns+ ',' ,'') + @InsertColumns

		IF EXISTS (SELECT * FROM Meta.config.edwTableJoins WHERE TableID=@TableID and (ShowBK is null or ShowBK=0))
		BEGIN 	
		SELECT @VarHideCols = (select string_agg(r.BusinessKeys, ',') 
			from Meta.config.edwTableJoins j
			inner join Meta.config.edwTables r on r.TableID=j.RelatedTableID
			where j.TableID=@TableID
			and (ShowBK is null or ShowBK=0))
		SET @HideColumns = coalesce(@HideColumns+ ',' ,'') +@VarHideCols				
		END

		SELECT @PrimaryKey = coalesce(@PrimaryKey, replace(@TargetTable, 'Dim','') + 'Key')
		,@InsertColumns = case when @InsertColumns is null then '' else @InsertColumns + ',' end + (SELECT string_agg( c.name, ',') FROM sys.columns c WHERE c.object_id = object_id(@SourceObject) and c.name NOT IN (SELECT ltrim(value) from string_split(@HideColumns,','))   )
		,@SelectColumns = case when @SelectColumns is null then '' else @SelectColumns + ',' end + (SELECT string_agg(  @TablePrefix + '.' + c.name + CASE WHEN @ForceCollation = 1 AND c.collation_name <> @TargetCollation THEN ' COLLATE ' + @TargetCollation + ' AS ' + c.name ELSE '' END, ',') FROM sys.columns c WHERE c.object_id = object_id(@SourceObject)  and c.name NOT IN (SELECT ltrim(value) from string_split(@HideColumns ,',')  ) )

		IF @PrestageSourceFlag=1
			BEGIN
				exec [dwa].[usp_TableLoad_PrestageView] @TargetObject = @TargetObject,@TableID =@TableID
				SET @SourceObject = @PrestageTargetObject +'_staging'
			END			
		PRINT '/*' + char(13) + char(10) + 'exec dwa.usp_TableLoad @TargetObject=''' + @TargetObject+''',@TableID=' + convert(varchar, @TableID) + ',@RunID=''' + convert (varchar(255), @RunID) +'''' +  char(13) + char(10) + '*/' ;
		
		IF @PrestageTargetFlag=1 AND object_id(@TargetObject) IS NOT NULL 
		BEGIN
			SET @CTAS=0 ; SET @Skip =1 ;			
			IF @LocalJoinSQL IS NOT NULL
				BEGIN 		
					SET @PrestageJoinSQL ='LEFT JOIN ' + @TargetObject + ' t ON '
					SET @PrestageJoinSQL = @PrestageJoinSQL + (SELECT string_agg(rtrim(bk.value), ' AND ') FROM string_split(rtrim(@LocalJoinSQL), ' ') bk)
				END
			 ELSE	
				BEGIN
					SET @PrestageJoinSQL ='LEFT JOIN ' + @TargetObject + ' t ON '
					SET @PrestageJoinSQL = @PrestageJoinSQL + (SELECT string_agg(@TablePrefix + '.' + ltrim(bk.value) +'=t.' + ltrim(bk.value),' AND ') FROM string_split(@BusinessKeys, ',') bk)  
				END

			SET @InsertFlag =1;	SET @AutoDrop=0	;			
			IF object_id(@PrestageTargetObject) IS NOT NULL 				
			BEGIN
				SET @sql ='DROP TABLE ' + @PrestageTargetObject
				PRINT @sql
				EXEC(@sql)	
			END					   
			BEGIN
				IF @PrestageJoinSQL is  null
					BEGIN
					SET @SelectColumns=@SelectColumns + ',''I'' as RowOperation'  
					IF @DeleteDDL is not null
						SET @sqlWhere= coalesce(@sqlWhere + char(13) + char(9) + ' AND ',char(13) + 'WHERE ') + 'NOT (' + @TablePrefix + '.' + @DeleteDDL + ')'  
					END
				ELSE
					BEGIN
					SET @SelectColumns=@SelectColumns + ',CASE ' + coalesce('WHEN ' + @TablePrefix + '.' + @DeleteDDL + ' THEN ''D''' ,'')  + ' WHEN t.RowChecksum is null then ''I'' WHEN t.RowChecksum <> ' + @TablePrefix + '.RowChecksum THEN ''U'' ELSE ''D'' END as RowOperation'  
					SET @SqlWhere=coalesce( @sqlWhere + char(13) +  ' AND ' , 'WHERE ') + '((t.RowChecksum is null OR t.RowChecksum <> ' + @TablePrefix + '.RowChecksum ' + coalesce(' AND NOT(' + @TablePrefix + '.' + @DeleteDDL+ ')','')   + ')' 
					IF @UpdateFlag=1 
						SET @sqlWhere= @sqlWhere + char(13) + char(9) +' OR (t.RowChecksum <> ' + @TablePrefix + '.RowChecksum)'
					IF @DeleteDDL is not null
						SET @sqlWhere= @sqlWhere +char(13) + char(9) + ' OR (' + @TablePrefix + '.' + @DeleteDDL + ' AND t.RowChecksum is not null)'
					SET @sqlWhere= @sqlWhere+ ')'
					END
			END
			
			/*PrestageTable - CTAS*/
			EXEC [dwa].[usp_TableLoad_CTAS] @TableID = @TableID, @TargetObject=@PrestageTargetObject,@SourceObject=@SourceObject ,@SelectColumns=@SelectColumns,@JoinSQL=@JoinSQL,@SqlWhere=@SqlWhere,@PrestageJoinSQL=@PrestageJoinSQL,@DeltaJoinSql=@DeltaJoinSql,@RelatedBusinessKeys=@RelatedBusinessKeys	
		END

		IF @RelatedTableCount >1
				SET @UpdateColumns = (	SELECT string_agg ('t.' + c.name + '=' + @TablePrefix + '.' + c.name  , ',') FROM sys.columns c WHERE c.is_identity=0 AND c.object_id = object_id(@TargetObject) 
								AND c.name not in (SELECT value from string_split(@BusinessKeys,','))
							    AND c.name NOT IN (SELECT ltrim(value) from string_split(@HideColumns,','))
				)
		ELSE
				SET @UpdateColumns = (	SELECT string_agg ('t.' + c.name + '=' + @TablePrefix + '.' + c.name  , ',') FROM sys.columns c WHERE c.is_identity=0 AND c.object_id = object_id(@SourceObject) 
								AND c.name not in (SELECT value from string_split(@BusinessKeys,','))
								AND c.name NOT IN (SELECT ltrim(value) from string_split(@HideColumns,',')))

		IF @PrestageTargetFlag=1 AND object_id(@TargetObject) IS NOT NULL 
		BEGIN 	
			EXEC [dwa].[usp_TableLoad_Insert] @TableID=@TableID,@TargetObject= @TargetObject,@SourceObject=@PrestageTargetObject,@InsertColumns=@InsertColumns,@SelectColumns=@SelectColumns, @JoinSQL=NULL, @SqlWhere=NULL, @WhereJoinSQL= NULL, @Exists=@Exists;
			EXEC [dwa].[usp_TableLoad_Update] @TableID=@TableID,@TargetObject= @TargetObject,@SourceObject=@PrestageTargetObject,@UpdateColumns=@UpdateColumns,@SelectColumns=@SelectColumns, @JoinSQL=NULL, @SqlWhere=@Sqlwhere, @WhereJoinSQL= @WhereJoinSQL, @Exists=@Exists;		
		END
		
		/*CTAS*/
		IF @CTAS =1 and @Skip=0		
			EXEC [dwa].[usp_TableLoad_CTAS] @TableID = @TableID, @TargetObject=@TargetObject,@SourceObject=@SourceObject ,@SelectColumns=@SelectColumns,@JoinSQL=@JoinSQL,@SqlWhere=@SqlWhere,@PrestageJoinSQL=@PrestageJoinSQL,@DeltaJoinSql=@DeltaJoinSql,@RelatedBusinessKeys=@RelatedBusinessKeys;
		ELSE IF @Skip=0
		BEGIN
			IF OBJECT_ID(@TargetObject) IS NULL
				EXEC [dwa].usp_TableCreate @TableID;

			IF @DeleteDDL IS NOT NULL
				SET @SqlWhere = coalesce(@SqlWhere + ' AND ', 'WHERE ') + 'NOT (' + @TablePrefix + '.' + @DeleteDDL + ')'			
			/*INSERT*/
			IF @InsertFlag=1
			BEGIN 		
				EXEC [dwa].[usp_TableLoad_Insert] @TableID=@TableID,@TargetObject= @TargetObject,@SourceObject=@SourceObject,@InsertColumns=@InsertColumns,@SelectColumns=@SelectColumns, @JoinSQL=@JoinSQL, @SqlWhere=@sqlwhere, @WhereJoinSQL= @WhereJoinSQL, @Exists=@Exists
			END				
		END 	
				
		/*UPDATE*/
		IF coalesce(@UpdateFlag,1) =1 and @Exists=1 and @CTAS =0  and @Skip=0	
		BEGIN
			IF @DeleteDDL is not null 				
				SET @SqlWhere ='WHERE NOT (' + @TablePrefix + '.' + @DeleteDDL + ')'
			BEGIN 	
				EXEC [dwa].[usp_TableLoad_Update] @TableID=@TableID,@TargetObject= @TargetObject,@SourceObject=@SourceObject,@UpdateColumns=@UpdateColumns,@SelectColumns=@SelectColumns, @JoinSQL=@JoinSQL, @SqlWhere=@sqlwhere, @WhereJoinSQL= @WhereJoinSQL, @Exists=@Exists
			END				
		END	

		/*DELETE*/
		IF coalesce(@DeleteFlag,1) =1 and @Exists=1 and @DeleteDDL is not null and @Skip=0
			EXEC [dwa].[usp_TableLoad_Delete] @TableID=@TableID ,@SelectColumns=@SelectColumns,@JoinSQL=@JoinSQL 

	END
	ELSE IF @Skip=0
		RAISERROR ('Unsupported SourceType=%s. Check Meta.config.edwTables.SourceType',16,1,@SourceType)

		IF @PostLoadSQL IS NOT NULL
		BEGIN
			PRINT '--PostLoadSQL Event'
			PRINT @PostLoadSQL
			PRINT ''
			EXEC (@PostLoadSQL)
		END
		
	END TRY

	BEGIN CATCH
		DECLARE @ErrorNumber INT, @ErrorProcedure varchar(128), @ErrorMessage varchar(4000)
		SELECT @ErrorNumber = ERROR_NUMBER(), @ErrorProcedure = ERROR_PROCEDURE(), @ErrorMessage = ERROR_MESSAGE();
		RAISERROR ('Error in %s object. Check error message ''%s''', 16, 1, @ErrorProcedure, @ErrorMessage)
	END CATCH
END
GO


/****** Object:  StoredProcedure [int].[usp_LoadNumbers]    Script Date: 10/03/2025 19:22:15 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/*   Description: Create Numbers Tables
     History:    
		20/02/2025 Created                
 */
CREATE PROC [int].[usp_LoadNumbers] AS
BEGIN
    SET NOCOUNT ON;
  
    IF OBJECT_ID('int.Numbers') IS NOT NULL
    BEGIN
        DROP TABLE int.Numbers
    END

   BEGIN
        CREATE TABLE int.Numbers(
            [n] [bigint]  NULL
        )        
    END

   ;WITH  L0   AS (SELECT 1 AS n UNION ALL SELECT 1),  
    L1   AS (SELECT 1 AS n FROM L0 AS a CROSS JOIN L0 AS b),  
    L2   AS (SELECT 1 AS n FROM L1 AS a CROSS JOIN L1 AS b),  
    L3   AS (SELECT 1 AS n FROM L2 AS a CROSS JOIN L2 AS b),  
    L4   AS (SELECT 1 AS n FROM L3 AS a CROSS JOIN L3 AS b),  
    L5   AS (SELECT 1 AS n FROM L4 AS a CROSS JOIN L4 AS b),  
    Nums AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM L5)



   INSERT INTO int.Numbers(n)
    SELECT TOP(10958) n FROM Nums ORDER BY n;
END
GO


/****** Object:  StoredProcedure [int].[usp_SettingEndDate]    Script Date: 10/03/2025 19:22:28 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/* 
	Description: Set EndDate for populating DimDate on the basis on max date from Trans table     
	History:
		20/02/2025  Created                
*/
CREATE PROC [int].[usp_SettingEndDate] AS
BEGIN
    SET NOCOUNT ON;
   
    DECLARE @EndDate AS INT;
	DECLARE @MaxDate AS DATE;

	SET @MaxDate = (
			SELECT COALESCE(MAX(Date),getdate())
			FROM tst.Finance
			);
	SET @EndDate = (
			SELECT CONVERT(VARCHAR(8), DATEADD(yy, DATEDIFF(yy, 0, MAX(DATEADD(d, 7, CONVERT(DATE, @MaxDate)))) + 1, - 1), 112) AS EndDate
			);

	UPDATE config.Settings
	SET [Value] = @EndDate
	WHERE Entity = 'EDW'
		AND Attribute = 'EndDate';
END
GO


