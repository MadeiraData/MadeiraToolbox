/*
Author: Eitan Blumin | https://www.eitanblumin.com
Create Date: 2020-03-18
Description:
  This script will detect currently running sessions in your database which are running DBCC SHRINK commands.
  It will also output the name of any tables and indexes the session is currently locking.
  
  Use this query to find out what causes a SHRINK to run for too long.
  You may need to run it multiple times to "catch" the relevant info.
*/
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