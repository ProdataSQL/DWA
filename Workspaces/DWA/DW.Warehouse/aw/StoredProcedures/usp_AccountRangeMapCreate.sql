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