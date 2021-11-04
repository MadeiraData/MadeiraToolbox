SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @MinVLFCountForAlert int, @majorver int
SET @MinVLFCountForAlert = 300;
SET @majorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff);

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
 
 SELECT msg = 'In server: ' + @@SERVERNAME + ', database: ' + QUOTENAME(dbname) + ' has a high VLF count (potential VLF count: '
 + CONVERT(nvarchar(MAX), potential.PotentialVLFCount) + N')'
 , VLF_Count
 , LogSizeMB = m.size_mb
 , PotentialSizeMB = iter.potsize
 , potential.PotentialVLFCount
 FROM @vlfcounts
 CROSS APPLY (SELECT SUM(size) / 128 FROM sys.master_files AS mf WHERE mf.database_id = DB_ID(dbname) AND mf.type_desc = 'LOG') AS m(size_mb)
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
	WHERE @majorver >= 11

	UNION ALL

	SELECT n_iter = (SELECT CASE WHEN m.size_mb <= 64 THEN 1
			WHEN m.size_mb > 64 AND m.size_mb < 256 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/256, 0)
			WHEN m.size_mb >= 256 AND m.size_mb < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/512, 0)
			WHEN m.size_mb >= 1024 AND m.size_mb < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/1024, 0)
			WHEN m.size_mb >= 4096 AND m.size_mb < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/2048, 0)
			WHEN m.size_mb >= 8192 AND m.size_mb < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/4000, 0)
			WHEN m.size_mb >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(m.size_mb, -2))/8000, 0)
			END)
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

 SELECT msg = 'In server: ' + @@SERVERNAME + ', database: ' + QUOTENAME(d.[name]) + ' has a high VLF count (potential VLF count: '
 + CONVERT(nvarchar(MAX), potential.PotentialVLFCount) + N')'
 , VLF_Count = vlf.total_vlf_count
 , LogSizeMB = m.size_mb
 , PotentialSizeMB = iter.potsize
 , potential.PotentialVLFCount
 FROM sys.databases d
 CROSS APPLY sys.dm_db_log_stats(database_id) AS vlf
 CROSS APPLY (SELECT SUM(size) / 128 FROM sys.master_files AS mf WHERE mf.database_id = d.database_id AND mf.type_desc = 'LOG') AS m(size_mb)
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
 WHERE d.database_id > 4
 AND [state] = 0 AND is_read_only = 0
 AND vlf.total_vlf_count > @MinVLFCountForAlert
 AND vlf.total_vlf_count > potential.PotentialVLFCount
 OPTION (RECOMPILE);
END
