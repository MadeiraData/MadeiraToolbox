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

	You can also use this to generate a log resizing command,
	for the purpose of reducing VLF count.
*/
SET NOCOUNT ON;
SELECT
	DatabaseName		= db.name
      , File_Path		= f.physical_name
      , File_Size_MB		= f.size / 128
      , Shrinkable_Log_MB	= lstat.VLFSize
      , Shrinkable_VLFs		= lstat.VLFCount
      , Total_VLFs		= lstat.VLFTotalCount
      , PotentialSizeMB		= iter.potsize
      , PotentialVLFCount	= potential.PotentialVLFCount
      , LastLogBackup		= ldetails.log_backup_time
      , ShrinkCmd		= N'USE ' + QUOTENAME(db.name) + N'; CHECKPOINT; DBCC SHRINKFILE ('
				+ QUOTENAME(f.name) + N' , 0, TRUNCATEONLY) WITH NO_INFOMSGS;'
      
      , ResizeCmd		= N'USE ' + QUOTENAME(db.name) + N'; CHECKPOINT; DBCC SHRINKFILE ('
				+ QUOTENAME(f.name) + N' , 0, TRUNCATEONLY) WITH NO_INFOMSGS; '
				+ N'USE [master]; ALTER DATABASE ' + QUOTENAME(db.name)
		+ ' MODIFY FILE ( NAME = N' + QUOTENAME(f.name, '''') + ', SIZE = ' + CONVERT(nvarchar(max), iter.potsize) + N'MB );'
FROM sys.databases AS db
INNER JOIN sys.master_files AS f ON db.database_id = f.database_id AND f.type_desc = 'LOG'
CROSS APPLY sys.dm_db_log_stats (db.database_id) AS ldetails
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
CROSS APPLY (SELECT f.size / 128) AS m(size_mb)
CROSS APPLY (
	SELECT n_iter = (SELECT CASE WHEN m.size_mb <= 64 THEN 1
			WHEN m.size_mb > 64 AND m.size_mb < 256 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/256, 0)
			WHEN m.size_mb >= 256 AND m.size_mb < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/512, 0)
			WHEN m.size_mb >= 1024 AND m.size_mb < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/1024, 0)
			WHEN m.size_mb >= 4096 AND m.size_mb < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/2048, 0)
			WHEN m.size_mb >= 8192 AND m.size_mb < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/4096, 0)
			WHEN m.size_mb >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/8192, 0)
			END)
	 , potsize = (SELECT CASE WHEN m.size_mb <= 64 THEN 1*64
			WHEN m.size_mb > 64 AND m.size_mb < 256 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/256, 0)*256
			WHEN m.size_mb >= 256 AND m.size_mb < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/512, 0)*512
			WHEN m.size_mb >= 1024 AND m.size_mb < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/1024, 0)*1024
			WHEN m.size_mb >= 4096 AND m.size_mb < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/2048, 0)*2048
			WHEN m.size_mb >= 8192 AND m.size_mb < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/4096, 0)*4096
			WHEN m.size_mb >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/8192, 0)*8192
			END)
) AS iter
CROSS APPLY (SELECT PotentialVLFCount = CASE WHEN iter.potsize <= 64 THEN (iter.potsize/(iter.potsize/iter.n_iter))*4
			WHEN iter.potsize > 64 AND iter.potsize < 1024 THEN (iter.potsize/(iter.potsize/iter.n_iter))*8
			WHEN iter.potsize >= 1024 THEN (iter.potsize/(iter.potsize/iter.n_iter))*16
			END) AS potential
WHERE
	db.recovery_model_desc = 'FULL'
	AND db.database_id > 4
	AND HAS_DBACCESS(db.name) = 1
	AND DATABASEPROPERTYEX(db.name, 'Updateability') = 'READ_WRITE'
	--AND lstat.VLFTotalCount > 300
ORDER BY Total_VLFs DESC, Shrinkable_Log_MB DESC;
