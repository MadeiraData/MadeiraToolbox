SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @MinVLFCountForAlert INT
SET @MinVLFCountForAlert = 300;

IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) < 13
BEGIN
 --table variable to hold results  
 DECLARE @vlfcounts AS TABLE(DBname SYSNAME,VLF_Count INT);
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
  IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) < 11
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
 
 SELECT 'In server: ' + @@SERVERNAME + ', database: ' + QUOTENAME(dbname) + ' has a high VLF count', VLF_Count
 FROM @vlfcounts
 WHERE VLF_Count > @MinVLFCountForAlert
END
ELSE
BEGIN 

 SELECT 'In server: ' + @@SERVERNAME + ', database: ' + QUOTENAME(d.[name]) + ' has a high VLF count', vlf.total_vlf_count
 FROM sys.databases d
 CROSS APPLY sys.dm_db_log_stats(database_id) AS vlf
 WHERE d.database_id > 4
 AND [state] = 0 AND is_read_only = 0
 AND vlf.total_vlf_count > @MinVLFCountForAlert
 OPTION (RECOMPILE);
END
