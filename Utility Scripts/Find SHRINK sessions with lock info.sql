/*
Author: Eitan Blumin | https://www.eitanblumin.com
Create Date: 2020-03-18
Description:
  This script will detect currently running sessions in your database which are running DBCC SHRINK commands.
  It will also output the name of any tables and indexes the session is currently locking.
  
  Use this query to find out what causes a SHRINK to run for too long.
  You may need to run it multiple times to "catch" the relevant info.
  Optionally, set @RunUntilCaughtLockInfo to 1 to continuously run until a session with object lock info was caught.
*/
DECLARE @RunUntilCaughtLockInfo BIT = 0

DECLARE @Results TABLE
(
	session_id int null,
	start_time datetime null,
	command nvarchar(max) null,
	[status] sysname null,
	wait_time int null,
	wait_type sysname null,
	[dbid] int null,
	dbname sysname null,
	[objid] int null,
	SchemaName sysname null,
	TableName sysname null,
	IndexId int null,
	IndexName sysname null
);

WHILE 1=1
BEGIN

	INSERT INTO @Results
	SELECT DISTINCT
		req.session_id,
		req.start_time,
		req.command,
		req.status,
		req.wait_time,
		req.wait_type,
		ISNULL(rsc_dbid, req.database_id) AS dbid,
		DB_NAME(ISNULL(rsc_dbid, req.database_id)) AS dbname,
		rsc_objid AS ObjId,
		OBJECT_SCHEMA_NAME(rsc_objid, rsc_dbid) AS SchemaName,
		OBJECT_NAME(rsc_objid, rsc_dbid) AS TableName,
		rsc_indid As IndexId,
		indexes.name AS IndexName
	FROM sys.dm_exec_requests AS req
	LEFT JOIN master.dbo.syslockinfo ON req_spid = req.session_id AND rsc_objid <> 0
	LEFT JOIN sys.indexes ON syslockinfo.rsc_objid = indexes.object_id AND syslockinfo.rsc_indid = indexes.index_id
	WHERE req.command IN ('DbccFilesCompact', 'DbccSpaceReclaim')
	OPTION(RECOMPILE);

	IF @@ROWCOUNT = 0 AND @RunUntilCaughtLockInfo = 1 CONTINUE;
	IF @RunUntilCaughtLockInfo = 0 BREAK;

	IF NOT EXISTS (SELECT * FROM @Results WHERE [objid] IS NOT NULL)
	BEGIN
		DELETE @Results;
		CONTINUE;
	END
	ELSE
	BEGIN
		BREAK;
	END

END

SELECT *
FROM @Results;
