/*
Find All Shrinkable Transaction Logs
====================================
Author: Eitan Blumin
Date: 2021-10-16
Description:
	This script can be used to find all transaction log files
	that don't have active VLFs at their tail, making it possible
	to shrink the file easily using TRUNCATEONLY.

	This script is ideal for when you have a lot of databases
	and you're running out of disk space, and need a quick way
	to find which databases can be shrunk as soon as possible.

	WARNING: It is strongly adviseable NOT to shrink files,
	especially transaction log files.
	This script is for emergencies ONLY.
*/
SET NOCOUNT ON;
SELECT
	DatabaseName		= db.name
      , File_Path		= f.physical_name
      , File_Size_MB		= f.size / 128
      , Shrinkable_Log_MB	= lstat.VLFSize
      , Shrinkable_VLFs		= lstat.VLFCount
      , Total_VLFs		= lstat.VLFTotalCount
      , ShrinkCmd		= N'USE ' + QUOTENAME(db.name) + N'; CHECKPOINT; DBCC SHRINKFILE ('
				+ CONVERT(nvarchar(MAX), f.file_id) + N' , 0, TRUNCATEONLY) WITH NO_INFOMSGS;'
FROM sys.databases AS db
INNER JOIN sys.master_files AS f ON db.database_id = f.database_id AND f.type_desc = 'LOG'
CROSS APPLY
(
	SELECT TOP (1) *
	FROM
	(
		SELECT *
		      , VLFSize		= SUM(vlf_size_mb) OVER (ORDER BY vlf_begin_offset DESC)
		      , VLFCount	= COUNT(vlf_active) OVER (ORDER BY vlf_begin_offset DESC)
		      , VLFTotalCount	= COUNT(vlf_active) OVER ()
		      , VLFActives	= MAX(CONVERT(int, vlf_active)) OVER (ORDER BY vlf_begin_offset DESC)
		FROM	sys.dm_db_log_info(db.database_id) AS li
		WHERE	li.file_id = f.file_id
	) AS linfo
	WHERE	VLFActives = 0
	ORDER BY vlf_active ASC
) AS lstat
WHERE
	db.recovery_model_desc = 'FULL'
	AND db.database_id > 4
	AND HAS_DBACCESS(db.name) = 1
	AND DATABASEPROPERTYEX(db.name, 'Updateability') = 'READ_WRITE'
ORDER BY Shrinkable_Log_MB DESC;
