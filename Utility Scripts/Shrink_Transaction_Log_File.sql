-- This script attempts to shrink current database's transaction log
-- by truncating from the transaction log tail only
-- and only if there's no active VLF at the tail of the transaction log file.
-- Use this script when trying to reduce VLF count but without changing to SIMPLE recovery model
-- and without attempting to re-organize the transaction log.
-- Supported versions: SQL Server 2016 SP 2 and later 
DECLARE @MinimumLogSizeMB int = 8;
DECLARE @DBId int = DB_ID();

DECLARE
	@FileName	 sysname
      , @IsLastVLFActive tinyint
      , @CMD		 nvarchar(MAX);

WHILE @MinimumLogSizeMB < (SELECT SUM(total_log_size_mb) FROM sys.dm_db_log_stats(@DBId))
BEGIN
	SELECT	TOP (1)
		@FileName	 = mf.name
	      , @IsLastVLFActive = inf.vlf_active
	FROM
		sys.dm_db_log_info(@DBId) AS inf
	INNER JOIN sys.master_files	  AS mf ON inf.database_id = mf.database_id AND inf.file_id = mf.file_id
	ORDER BY inf.vlf_begin_offset DESC;

	IF @IsLastVLFActive <> 0
	BEGIN
		PRINT N'Last VLF is still active. Aborting.'
		BREAK;
	END

	SET @CMD = N'USE ' + QUOTENAME(DB_NAME(@DBId)) + N'; CHECKPOINT; DBCC SHRINKFILE (N' + QUOTENAME(@FileName, N'''') + N' , 0, TRUNCATEONLY) WITH NO_INFOMSGS;';
	PRINT @CMD;
	EXEC (@CMD);
END;

SELECT
	log_since_last_log_backup_mb
      , log_backup_time
      , active_log_size_mb
      , total_log_size_mb
      , total_vlf_count
FROM	sys.dm_db_log_stats(@DBId);