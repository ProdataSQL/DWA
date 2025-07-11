/*********************************************************************************************
Fabric sp_WhoIsActive by Bob Duffy. bob@prodata.ie

This is based on the SqlDbEngine sp_WhoIsActive 
(C) 2007-2018, Adam Machanic
http://whoisactive.com

*********************************************************************************************/
CREATE   PROCEDURE [dbo].[sp_whoisactive]
AS
BEGIN
    SET NOCOUNT ON
	DECLARE @sql varchar(4000)
	DECLARE @queryHashJson varchar(8000)
	DECLARE @queryTimeJson varchar(8000)
	
	--Cache last execution times into JSON as not supported to Join System DMVs and user tables like QueryInsights
	--Todo: QueryInsights has seperate tables per database, but we only check active one.
	SELECT @queryHashJson='[' + string_agg ('{"query_hash":"' + replace(upper(sys.fn_varbintohexstr( query_hash)),'0X','0x')+ '"}',',') + ']' 
	FROM sys.dm_exec_requests r
	INNER JOIN sys.dm_exec_sessions s on s.session_id =r.session_id
	WHERE r.session_id <> @@SPID
	AND s.program_name <> 'QueryInsights'

	;WITH rh as (
		SELECT distinct query_hash, FIRST_VALUE(total_elapsed_time_ms)  OVER (PARTITION BY query_hash ORDER BY start_time ) as time_ms 
		FROM queryinsights.exec_requests_history h
		WHERE query_hash IN 
		(SELECT query_hash FROM OPENJSON(@queryhashJson) WITH (query_hash varchar(4000)) x)
		AND status='Succeeded'
	)
	SELECT @queryTimeJson =  '[' + string_agg ('{"query_hash":"' + query_hash + '","time_ms":"' + convert(varchar(50),time_ms)  + '"}',',') + ']' 
	FROM rh

	/* Query DMVs and Join to QueryInsights cached times in Json */
	SELECT RIGHT('0' + CAST(r.total_elapsed_time / (1000 * 60 * 60 * 24) AS VARCHAR(10)),2) + ' ' + -- Days
    RIGHT('0' + CAST((r.total_elapsed_time / (1000 * 60 * 60)) % 24 AS VARCHAR(2)), 2) + ':' + -- Hours
    RIGHT('0' + CAST((r.total_elapsed_time / (1000 * 60)) % 60 AS VARCHAR(2)), 2) + ':' + -- Minutes
    RIGHT('0' + CAST((r.total_elapsed_time / 1000) % 60 AS VARCHAR(2)), 2) + '.' + -- Seconds
    RIGHT('00' + CAST(r.total_elapsed_time % 1000 AS VARCHAR(3)), 3) AS [dd hh:mm:ss.mss]
	, r.session_id
	, SUBSTRING(st.text, (r.statement_start_offset / 2)+1, ((CASE statement_end_offset WHEN -1 THEN DATALENGTH(st.text)ELSE r.statement_end_offset END-r.statement_start_offset)/ 2)+1)  AS sql_text
	, s.login_name
	, r.wait_type + ':' + CONVERT(varchar, r.wait_time) as wait_info
	, r.cpu_time as cpu
	, r.blocking_session_id
	, r.logical_reads as reads
	, r.writes
	, r.reads as physical_reads
	, r.status
	, r.open_transaction_count as open_tran_count
	, CASE WHEN r.percent_complete > 0 OR qt.time_ms is null THEN r.percent_complete else convert(decimal (9,2),r.total_elapsed_time /qt.time_ms * 100) END as percent_complete
	, CASE WHEN r.percent_complete > 0 OR coalesce(qt.time_ms,0)  < 100 THEN null else dateadd(ss,convert(int,qt.time_ms/1000), r.start_time)  END as eta_time
	, s.host_name
	, db_name(s.database_id) as database_name
	, s.program_name
	, r.start_time
	, s.login_time
	, r.request_id
	, r.query_plan_hash
	, r.query_hash
	, GETDATE() as collection_time
 	FROM sys.dm_exec_requests r
	CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) st
	INNER JOIN sys.dm_exec_sessions s on s.session_id =r.session_id
	LEFT JOIN OPENJSON(@queryTimeJson) WITH (
		query_hash varchar(50) ,
		time_ms float
	) qt ON convert(varbinary(max), qt.query_hash,1)=r.query_hash
	WHERE r.session_id <> @@SPID
	AND s.program_name <> 'QueryInsights'
END