
DECLARE
	  @MinVLFCountForAlert int = 300
	, @RunRemediation varchar(10) = '$(RunRemediation)'

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @majorver int
SET @majorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff);

DECLARE @Results AS TABLE
(
DatabaseName sysname,
LogFileName sysname,
VLF_Count int,
LogSizeMB int,
PotentialSizeMB int,
PotentialVLFCount int,
LastLogBackup datetime NULL
);

IF OBJECT_ID('sys.dm_db_log_stats') IS NULL
BEGIN
 --table variable to hold results  
 DECLARE @vlfcounts AS TABLE(dbname SYSNAME,VLF_Count INT);
 DECLARE @dbccloginfo AS TABLE
 (  
  RecoveryUnitId INT NULL, 
  fileid  SMALLINT NULL,  
  file_size BIGINT NULL,  
  start_offset BIGINT NULL,  
  fseqno  INT NULL,  
  [status] TINYINT NULL,  
  parity  TINYINT NULL,  
  create_lsn NUMERIC(25,0) NULL
 )

 DECLARE @dbname SYSNAME,@query NVARCHAR(1000),@count_VLF INT;
 DECLARE minor_crsr
 CURSOR LOCAL FAST_FORWARD
 FOR
 SELECT [name]
 FROM sys.databases
 WHERE database_id > 4
 AND [state] = 0 AND is_read_only = 0;
 
 SET @query = 'DBCC LOGINFO (@dbname) WITH NO_INFOMSGS'

 OPEN minor_crsr;
 FETCH NEXT FROM minor_crsr INTO @dbname;
  
 WHILE @@FETCH_STATUS = 0
 BEGIN
  IF @majorver < 11
  BEGIN
   INSERT @dbccloginfo
   (fileid, file_size, start_offset, fseqno, [status], parity, create_lsn)
   EXEC sp_executesql @query, N'@dbname sysname', @dbname
   SET @count_VLF = @@ROWCOUNT;
  END
  ELSE
  BEGIN
   INSERT @dbccloginfo
   EXEC sp_executesql @query, N'@dbname sysname', @dbname
   SET @count_VLF = @@ROWCOUNT;
  END


  INSERT @vlfcounts 
  VALUES(@dbname,@count_VLF);

  FETCH NEXT FROM minor_crsr INTO @dbname;
 END

 CLOSE minor_crsr;  
 DEALLOCATE minor_crsr;  
 
 INSERT INTO @Results
 SELECT dbname AS DatabaseName
 , mf.name AS LogFileName
 , VLF_Count
 , LogSizeMB = m.size_mb
 , PotentialSizeMB = iter.potsize
 , potential.PotentialVLFCount
 , LastLogBackup		= (SELECT MAX(bu.backup_start_date) FROM msdb.dbo.backupset AS bu WHERE bu.database_name = dbname AND bu.type IN ('L','D'))
 FROM @vlfcounts
 INNER JOIN sys.master_files AS mf
 ON mf.database_id = DB_ID(dbname) AND mf.type_desc = 'LOG'
 CROSS APPLY (SELECT mf.size / 128) AS m(size_mb)
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
	WHERE @majorver >= 11

	UNION ALL

	SELECT n_iter = NULLIF((SELECT CASE WHEN m.size_mb <= 64 THEN 1
			WHEN m.size_mb > 64 AND m.size_mb < 256 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/256, 0)
			WHEN m.size_mb >= 256 AND m.size_mb < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/512, 0)
			WHEN m.size_mb >= 1024 AND m.size_mb < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/1024, 0)
			WHEN m.size_mb >= 4096 AND m.size_mb < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/2048, 0)
			WHEN m.size_mb >= 8192 AND m.size_mb < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/4000, 0)
			WHEN m.size_mb >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/8000, 0)
			END), 0)
	 , potsize = (SELECT CASE WHEN m.size_mb <= 64 THEN 1*64
			WHEN m.size_mb > 64 AND m.size_mb < 256 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/256, 0)*256
			WHEN m.size_mb >= 256 AND m.size_mb < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/512, 0)*512
			WHEN m.size_mb >= 1024 AND m.size_mb < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/1024, 0)*1024
			WHEN m.size_mb >= 4096 AND m.size_mb < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/2048, 0)*2048
			WHEN m.size_mb >= 8192 AND m.size_mb < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/4000, 0)*4000
			WHEN m.size_mb >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/8000, 0)*8000
			END)
	WHERE @majorver < 11
 ) AS iter
 CROSS APPLY (SELECT PotentialVLFCount = CASE WHEN iter.potsize <= 64 THEN (iter.potsize/(iter.potsize/iter.n_iter))*4
			WHEN iter.potsize > 64 AND iter.potsize < 1024 THEN (iter.potsize/(iter.potsize/iter.n_iter))*8
			WHEN iter.potsize >= 1024 THEN (iter.potsize/(iter.potsize/iter.n_iter))*16
			END) AS potential
 WHERE VLF_Count > @MinVLFCountForAlert
 AND VLF_Count > potential.PotentialVLFCount
END
ELSE
BEGIN 

 INSERT INTO @Results
 SELECT d.[name] AS DatabaseName
 , mf.name AS LogFileName
 , VLF_Count = vlf.total_vlf_count
 , LogSizeMB = m.size_mb
 , PotentialSizeMB = iter.potsize
 , potential.PotentialVLFCount
 , LastLogBackup		= vlf.log_backup_time
 FROM sys.databases d
 CROSS APPLY sys.dm_db_log_stats(d.database_id) AS vlf
 INNER JOIN sys.master_files AS mf
 ON mf.database_id = d.database_id AND mf.type_desc = 'LOG'
 CROSS APPLY (SELECT mf.size / 128) AS m(size_mb)
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
 WHERE d.database_id > 4
 AND d.[state] = 0 AND d.is_read_only = 0
 AND vlf.total_vlf_count > @MinVLFCountForAlert
 AND vlf.total_vlf_count > potential.PotentialVLFCount
 OPTION (RECOMPILE);
END

SELECT *
, RemediationCmd = N'USE ' + QUOTENAME(DatabaseName) + '; DBCC SHRINKFILE (N' + QUOTENAME(LogFileName, '''') + ' , 0, TRUNCATEONLY) WITH NO_INFOMSGS; '
		+ N' USE [master]; ALTER DATABASE ' + QUOTENAME(DatabaseName)
		+ ' MODIFY FILE ( NAME = N' + QUOTENAME(LogFileName, '''')
		+ ', SIZE = ' + CONVERT(nvarchar(max),PotentialSizeMB) + N'MB );'
FROM @Results;

IF LEFT(@RunRemediation, 1) = 'Y'
BEGIN

DECLARE @CurrDB sysname, @CurrLogFileName sysname, @PotentialSizeMB int, @CMD nvarchar(max)

DECLARE Logs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT DatabaseName, LogFileName, PotentialSizeMB
FROM @Results

OPEN Logs

WHILE 1=1
BEGIN
	FETCH NEXT FROM Logs INTO @CurrDB, @CurrLogFileName, @PotentialSizeMB
	IF @@FETCH_STATUS <> 0 BREAK;

	BEGIN TRY
		SET @CMD = N'USE ' + QUOTENAME(@CurrDB) + '; DBCC SHRINKFILE (N' + QUOTENAME(@CurrLogFileName, '''') + ' , 0, TRUNCATEONLY) WITH NO_INFOMSGS;'
		RAISERROR(N'%s',0,1,@CMD);
		EXEC(@CMD);

		SET @CMD = N'USE [master]; ALTER DATABASE ' + QUOTENAME(@CurrDB)
		+ ' MODIFY FILE ( NAME = N' + QUOTENAME(@CurrLogFileName, '''')
		+ ', SIZE = ' + CONVERT(nvarchar(max),@PotentialSizeMB) + N'MB )'
		RAISERROR(N'%s',0,1,@CMD);
		EXEC(@CMD);
	END TRY
	BEGIN CATCH
		PRINT N'ERROR: ' + ERROR_MESSAGE()
	END CATCH
END

CLOSE Logs;
DEALLOCATE Logs;

END