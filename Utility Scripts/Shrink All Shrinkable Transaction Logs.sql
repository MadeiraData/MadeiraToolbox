/*
Find All Shrinkable Transaction Logs
====================================
Author: Eitan Blumin
Date: 2021-10-16
Last Update: 2023-06-26
Description:
	This script can be used to find all transaction log files
	that don't have active VLFs at their tail, making it possible
	to shrink the file easily using TRUNCATEONLY.

	This script is ideal for when you have a lot of databases
	and you're running out of disk space, and need a quick way
	to find which databases can be shrunk as soon as possible.

	!!! WARNING !!!
	It is strongly adviseable NOT to shrink files,
	especially transaction log files.
	This script is for EMERGENCIES ONLY.

	You can also use this to generate a log resizing command
	for the purpose of reducing VLF count.

	Use the new @DoAction parameter to also easily execute
	the requested action:
		None					- Will only return informational resultset without performing any action.
		Shrink					- Performs DBCC SHRINKFILE with TRUNCATEONLY.
		ShrinkAndResize			- Performs shrink with TRUNCATEONLY and also ALTER DATABSE MODIFY FILE to set the file size to PotentialSizeMB.
		BackupToNulAndShrink	- Runs BACKUP LOG to DISK='NUL' in order to force-empty the transaction log file, then shrink with TRUNCATEONLY.
		!!!!!!!! WARNING !!!!!!!!
			Running action BackupToNulAndShrink will result in breaking the backup log chain !
			You MUST run a FULL backup after this operation in order to initialize a new backup log chain !!!
*/
DECLARE
	  @ShowShrinkableLogsOnly	bit = 1				-- Set to 1 to only show transaction logs that can be shrunk at the tail. Set to 0 for all.
	, @MinimumFileSizeMB		int = 256			-- Return transaction logs with this minimum MB size only. Set to NULL for all.
	, @MinimumTailLogMB			int = 128			-- Return transaction logs with this minimum MB tail size only. Set to NULL for all.
	, @DoAction					sysname = 'None'	-- Supported values: None | Shrink | ShrinkAndResize | BackupToNulAndShrink
													-- !!!! WARNING !!!! Running action BackupToNulAndShrink will result in breaking the backup log chain!


SET NOCOUNT ON;
DECLARE @Results AS TABLE
(
	DatabaseName			sysname,
	RecoveryModel			sysname,
	Logical_FileName		sysname,
	File_Path				sysname,
	File_Size_MB			float,
	IsShrinkable			bit,
	Tail_Log_MB				float,
	Tail_VLFs				int,
	Total_VLFs				int,
	PotentialSizeMB			float,
	PotentialVLFCount		int,
	LastLogBackup			datetime NULL,
	LogActiveSizeMB			float NULL,
	LogSinceBackupMB		float NULL,
	ShrinkCmd				nvarchar(max),
	ShrinkAndResizeCmd		nvarchar(max),
	BackupToNulAndShrinkCmd	nvarchar(max)
)

INSERT INTO @Results
OUTPUT inserted.*
SELECT
	    DatabaseName		= db.name
      , RecoveryModel		= db.recovery_model_desc
      , Logical_FileName	= f.[name]
      , File_Path		= f.physical_name
      , File_Size_MB		= f.size / 128
      , IsShrinkable		= CONVERT(bit, CASE WHEN VLFActives = 0 THEN 1 ELSE 0 END)
      , Tail_Log_MB		= lstat.VLFSize
      , Tail_VLFs		= lstat.VLFCount
      , Total_VLFs		= lstat.VLFTotalCount
      , PotentialSizeMB		= iter.potsize
      , PotentialVLFCount	= potential.PotentialVLFCount
      , LastLogBackup		= NULLIF(ldetails.log_backup_time, '1900-01-01 00:00:00.000')
      , LogActiveSizeMB		= ldetails.active_log_size_mb
      , LogSinceBackupMB	= ldetails.log_since_last_log_backup_mb
      , ShrinkCmd		= N'USE ' + QUOTENAME(db.name) + N'; CHECKPOINT; DBCC SHRINKFILE ('
				+ QUOTENAME(f.name) + N' , 0, TRUNCATEONLY) WITH NO_INFOMSGS;'
      
      , ShrinkAndResizeCmd		= N'USE ' + QUOTENAME(db.name) + N'; CHECKPOINT; DBCC SHRINKFILE ('
				+ QUOTENAME(f.name) + N' , 0, TRUNCATEONLY) WITH NO_INFOMSGS; '
				+ N'USE [master]; ALTER DATABASE ' + QUOTENAME(db.name)
			+ ' MODIFY FILE ( NAME = N' + QUOTENAME(f.name, '''') + ', SIZE = ' + CONVERT(nvarchar(max), iter.potsize) + N'MB );'
      , BackupToNulAndShrinkCmd		= N'USE ' + QUOTENAME(db.name) + N'; CHECKPOINT; '
				+ CASE WHEN db.recovery_model_desc <> 'SIMPLE' THEN N'BACKUP LOG ' + QUOTENAME(db.name) + N' TO DISK = N''NUL'' WITH COMPRESSION, STATS=5;' ELSE N'' END
				+ N'DBCC SHRINKFILE ('
				+ QUOTENAME(f.name) + N' , 0, TRUNCATEONLY) WITH NO_INFOMSGS;'
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
	WHERE	(@ShowShrinkableLogsOnly = 0 OR VLFActives = 0)
	ORDER BY CASE WHEN VLFActives = 0 THEN 0 ELSE 1 END ASC, vlf_active ASC
) AS lstat
CROSS APPLY (SELECT f.size / 128) AS m(size_mb)
CROSS APPLY (
	SELECT n_iter = NULLIF((SELECT CASE WHEN m.size_mb <= 64 THEN 1
			WHEN m.size_mb > 64 AND m.size_mb < 256 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/256, 0)
			WHEN m.size_mb >= 256 AND m.size_mb < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/512, 0)
			WHEN m.size_mb >= 1024 AND m.size_mb < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/1024, 0)
			WHEN m.size_mb >= 4096 AND m.size_mb < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/2048, 0)
			WHEN m.size_mb >= 8192 AND m.size_mb < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/4096, 0)
			WHEN m.size_mb >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/8192, 0)
			END), 0)
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
	HAS_DBACCESS(db.name) = 1
	AND DATABASEPROPERTYEX(db.name, 'Updateability') = 'READ_WRITE'
	AND (@MinimumFileSizeMB IS NULL OR f.size / 128.0 >= @MinimumFileSizeMB)
	AND (@MinimumTailLogMB IS NULL OR lstat.VLFSize >= @MinimumTailLogMB)
	--AND db.database_id > 4		-- uncomment this to return user databases only
	--AND db.recovery_model_desc = 'FULL'	-- uncomment this to only return databases with FULL recovery model
	--AND lstat.VLFTotalCount > 300		-- uncomment this to filter transaction logs based on VLF count
ORDER BY Tail_Log_MB DESC, Total_VLFs DESC, File_Size_MB DESC;


IF @@ROWCOUNT > 0 AND @DoAction IN ('Shrink','ShrinkAndResize','BackupToNulAndShrink')
BEGIN
	DECLARE @CMD nvarchar(max);

	IF @DoAction = 'BackupToNulAndShrink'
		RAISERROR(N'
!!!!!!!! WARNING !!!!!!!!

	Running action BackupToNulAndShrink will result in breaking the backup log chain !
	You must run a FULL backup after this operation in order to initialize a new backup log chain !!!

!!!!!!!! WARNING !!!!!!!!
',11,1) WITH NOWAIT;

	DECLARE cmd CURSOR
	LOCAL FAST_FORWARD
	FOR
	SELECT
		CASE @DoAction
			WHEN 'Shrink' THEN ShrinkCmd
			WHEN 'ShrinkAndResize' THEN ShrinkAndResizeCmd
			WHEN 'BackupToNulAndShrink' THEN BackupToNulAndShrinkCmd
		END
	FROM @Results

	OPEN cmd;

	WHILE 1=1
	BEGIN
		FETCH NEXT FROM cmd INTO @CMD;
		IF @@FETCH_STATUS <> 0 BREAK;

		IF @CMD IS NOT NULL
		BEGIN
			RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;
			EXEC(@CMD);
		END
	END

	CLOSE cmd;
	DEALLOCATE cmd;
END


--EXEC xp_fixeddrives;
