-- Last-page insert contention detection
-- original: https://learn.microsoft.com/en-US/troubleshoot/sql/database-engine/performance/resolve-pagelatch-ex-contention#1-confirm-the-contention-on-pagelatch_ex-and-identify-the-contention-resource
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

DECLARE @CMD nvarchar(MAX)

IF (CONVERT(INT, SERVERPROPERTY('ProductMajorVersion')) >= 15)
BEGIN
	SET @CMD = N'
	SELECT
	Msg = CONCAT(N''PAGELATCH_EX contention found on database '', QUOTENAME(DB_NAME(page_info.database_id)),
	N'', table '', QUOTENAME(OBJECT_SCHEMA_NAME(page_info.[object_id], r.db_id)), ''.'', QUOTENAME(OBJECT_NAME(page_info.[object_id], r.db_id))
	, N'', index '', page_info.index_id, N'' (res '', er.wait_resource COLLATE database_default, N''). Consider enabling OPTIMIZE_FOR_SEQUENTIAL_KEY option for the index.'')
	, contentionCount = COUNT(er.session_id)
	FROM sys.dm_exec_requests AS er
		CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) AS st 
		CROSS APPLY sys.fn_PageResCracker (er.page_resource) AS r  
		CROSS APPLY sys.dm_db_page_info(r.[db_id], r.[file_id], r.page_id, ''DETAILED'') AS page_info
	WHERE er.wait_type = ''PAGELATCH_EX'' AND er.page_resource IS NOT NULL AND page_info.database_id > 4
	GROUP BY er.wait_resource, page_info.database_id, r.db_id, page_info.[object_id], page_info.index_id
	HAVING COUNT(er.session_id) > 5 AND MAX (er.wait_time) > 10'

	EXEC sp_executesql @CMD WITH RECOMPILE;

END
ELSE
BEGIN

    SELECT Msg = CONCAT(N'PAGELATCH_EX contention found on database ', QUOTENAME(DB_NAME(er.database_id)),
	N', resource ', er.wait_resource, N'. Run the following to find which table and index it is: DBCC TRACEON(3604); DBCC PAGE(' + replace(wait_resource,':',',') + ',3); DBCC TRACEOFF(3604);')
	, contentionCount = COUNT(er.session_id)
    FROM sys.dm_exec_requests er
    WHERE er.wait_type = 'PAGELATCH_EX' AND er.wait_resource IS NOT NULL AND er.database_id > 4
    GROUP BY er.database_id, er.wait_resource
    HAVING COUNT(er.session_id) > 5 AND MAX (er.wait_time) > 10
	OPTION (RECOMPILE);

END
