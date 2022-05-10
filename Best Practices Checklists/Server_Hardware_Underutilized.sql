DECLARE
 @MinimumMBFreeToAlert INT = 540000,
 @PercentAvailableToAlert INT = 80

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @SQLVersion FLOAT, @SQLEdition SMALLINT, @CPUCount SMALLINT, @HTRatio INT, @Sockets INT, @MemUsedMB BIGINT, @EditionMaxCPU SMALLINT, @EditionMaxMemMB BIGINT;

SELECT
 @SQLVersion = (CONVERT(VARCHAR, (@@microsoftversion / 0x1000000) & 0xff)),
 @SQLEdition = (CONVERT(INT,SERVERPROPERTY('EngineEdition')))

DECLARE @CMD NVARCHAR(MAX)
SET @CMD = N'SELECT @CPUCount = cpu_count, @HTRatio = hyperthread_ratio'

IF EXISTS (SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_os_sys_info') AND name = 'socket_count')
	SET @CMD = @CMD + N', @Sockets = socket_count'
  
SET @CMD = @CMD + N'
FROM sys.dm_os_sys_info WITH (NOLOCK) 
OPTION (RECOMPILE);'

EXEC sp_executesql @CMD
	, N'@CPUCount SMALLINT OUTPUT, @HTRatio INT OUTPUT, @Sockets INT OUTPUT'
	, @CPUCount OUTPUT, @HTRatio OUTPUT, @Sockets OUTPUT

--SELECT @CPUCount AS [@CPUCount], @HTRatio AS [@HTRatio], @Sockets AS [@Sockets]

SELECT @MemUsedMB = convert(int, value_in_use)
FROM sys.configurations
WHERE name = 'max server memory (MB)'
OPTION (RECOMPILE);

-- Standard Edition
IF @SQLEdition = 2
BEGIN  
 SELECT TOP (1)
  @EditionMaxCPU = MaxCPU, @EditionMaxMemMB = MaxMemGB * 1024
 FROM (VALUES
 (10.5, 4, 64),
 (11, 16, 64),
 (12, 16, 128),
 (99, 24, 128)
 ) AS VersionLimits(MaxVersion, MaxCPU, MaxMemGB)
 WHERE MaxVersion >= @SQLVersion
 ORDER BY MaxVersion ASC
END

-- Express Edition
IF @SQLEdition = 4
BEGIN 
 SELECT TOP (1)
  @EditionMaxCPU = MaxCPU, @EditionMaxMemMB = MaxMemMB
 FROM (VALUES
 (10.5, 1, 1024),
 (12, 4, 1024),
 (99, 4, 1410)
 ) AS VersionLimits(MaxVersion, MaxCPU, MaxMemMB)
 WHERE MaxVersion >= @SQLVersion
 ORDER BY MaxVersion ASC
END

SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#Volumes') IS NOT NULL DROP TABLE #Volumes;
IF OBJECT_ID('tempdb..#Files') IS NOT NULL DROP TABLE #Files;
CREATE TABLE #Volumes
(
 volume_mount_point sysname,
 total_bytes bigint,
 available_bytes bigint
);
CREATE TABLE #Files
(
 database_id int,
 file_id int,
 file_name sysname,
 volume_mount_point sysname,
 max_size bigint,
 size bigint,
 spaceused bigint
);

IF OBJECT_ID('sys.dm_os_volume_stats') IS NOT NULL
BEGIN
 SET @CMD = N'INSERT INTO #Volumes
 SELECT DISTINCT vs.volume_mount_point, vs.total_bytes'
 + CASE WHEN CONVERT(sysname, SERVERPROPERTY('Edition')) = 'SQL Azure' THEN N' + vs.available_bytes' ELSE N'' END
 + N', vs.available_bytes
 FROM (
  SELECT *
  FROM sys.master_files AS f WITH(NOLOCK)
  WHERE DATABASEPROPERTYEX(DB_NAME(f.database_id), ''Status'') = ''ONLINE''
  AND DATABASEPROPERTYEX(DB_NAME(f.database_id), ''Updateability'') = ''READ_WRITE''
  AND f.type IN (0,1)
  AND f.database_id <> 2
 ) AS f
 CROSS APPLY sys.dm_os_volume_stats (f.database_id, f.file_id)  AS vs
 --WHERE vs.volume_mount_point <> ''C:\'''
 EXEC(@CMD)

 DECLARE @Executor NVARCHAR(1000);
 SET @CMD = N'
 INSERT INTO #Files
 SELECT 
   DB_ID()
 , f.file_id
 , f.name
 , vs.volume_mount_point
 , f.max_size
 , f.size
 , FILEPROPERTY(f.[name], ''SpaceUsed'')
 FROM sys.database_files AS f
 CROSS APPLY sys.dm_os_volume_stats (DB_ID(), f.file_id)  AS vs
 WHERE f.type IN (0,1)
 '

 DECLARE @CurrDBId INT, @CurrDBName SYSNAME

 DECLARE DBs CURSOR
 LOCAL FAST_FORWARD
 FOR
 SELECT database_id, [name]
 FROM sys.databases
 WHERE [state] = 0
 AND database_id <> 2
 AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE';

 OPEN DBs
 WHILE 1=1
 BEGIN
  FETCH NEXT FROM DBs INTO @CurrDBId, @CurrDBName
  IF @@FETCH_STATUS <> 0
   BREAK;
  SET @Executor = QUOTENAME(@CurrDBName) + N'..sp_executesql'

  BEGIN TRY
  EXEC @Executor @CMD, N'@DBId INT', @CurrDBId
  END TRY
  BEGIN CATCH
	PRINT N'Error while getting free space details from ' + QUOTENAME(@CurrDBName)
	PRINT ERROR_MESSAGE()
  END CATCH
 END

 CLOSE DBs;
END

;WITH DiskData
AS
(
SELECT *
, total_available_mb = available_mb + unused_space_mb
, available_percent = (available_mb + unused_space_mb) * 100.0 / total_mb
FROM #Volumes AS vs
CROSS APPLY
(SELECT total_mb = total_bytes / 1024 / 1024
, available_mb = available_bytes / 1024 / 1024
) AS ex
CROSS APPLY
(
SELECT unused_space_mb = SUM(ISNULL(f.size - f.spaceused,0)) / 128 
FROM #Files AS f
WHERE vs.volume_mount_point = f.volume_mount_point
AND database_id <> 2
) AS d
)
SELECT ErrorMsg = N'In server: ' + QUOTENAME(@@SERVERNAME) + N' in volume ' + volume_mount_point
+ N' there are ' + CONVERT(nvarchar, total_available_mb) + N' MB available out of ' + CONVERT(nvarchar, total_mb) + N' MB ('
+ CONVERT(nvarchar, CONVERT(float, ROUND(available_percent, 2))) + N' %) '
, total_available_mb AS [value]
FROM DiskData
WHERE available_percent > @PercentAvailableToAlert
AND total_available_mb >= @MinimumMBFreeToAlert
AND total_bytes >= available_bytes

UNION ALL

SELECT ErrorMSG = N'This ' + CASE @SQLEdition WHEN 2 THEN N'Standard' WHEN 4 THEN N'Express' END 
+ N' Edition of SQL Server (version ' + CONVERT(nvarchar(max), @SQLVersion) + N') supports ' + Msg
, @SQLVersion
FROM (
 SELECT Msg = N'up to ' + CONVERT(nvarchar(max), @EditionMaxCPU) + N' CPU core(s), but ' + QUOTENAME(@@SERVERNAME) + N' has ' + CONVERT(nvarchar(max), @CPUCount/@HTRatio) + N' physical CPU cores.'
 WHERE @EditionMaxCPU < @CPUCount/@HTRatio

 UNION ALL

 SELECT Msg = N'up to ' + CONVERT(nvarchar(max), @EditionMaxMemMB) + N' MB max memory, but ' + QUOTENAME(@@SERVERNAME) + N' has ' + CONVERT(nvarchar(max), @MemUsedMB) + N' MB max memory configured.'
 WHERE @EditionMaxMemMB < @MemUsedMB

 UNION ALL

 SELECT Msg = N'up to 1 processor socket, but ' + QUOTENAME(@@SERVERNAME) + N' has ' + CONVERT(nvarchar(max), @Sockets) + N' processor sockets.'
 WHERE @SQLEdition = 4 AND @Sockets > 1

 UNION ALL

 SELECT Msg = N'up to 4 processor sockets, but ' + QUOTENAME(@@SERVERNAME) + N' has ' + CONVERT(nvarchar(max), @Sockets) + N' processor sockets.'
 WHERE @SQLEdition = 2 AND @Sockets > 4
) AS q
WHERE Msg IS NOT NULL

UNION ALL

SELECT N'In server: ' + QUOTENAME(@@SERVERNAME) + N' there are unused CPU schedulers found', COUNT(*) AS [value] 
FROM sys.dm_os_schedulers WITH (NOLOCK)
WHERE [is_online] = 0
and scheduler_id < 255
HAVING COUNT(*) > 0

UNION ALL

SELECT N'In server: ' + QUOTENAME(@@SERVERNAME) + N' there are offline CPU schedulers found', COUNT(*) AS [value] 
FROM sys.dm_os_schedulers WITH (NOLOCK)
WHERE [status] = N'VISIBLE OFFLINE'
and scheduler_id < 255
HAVING COUNT(*) > 0

UNION ALL

SELECT N'In server: ' + QUOTENAME(@@SERVERNAME) + N' there are CPU schedulers that failed to create workers', COUNT(*) AS [value] 
FROM sys.dm_os_schedulers WITH (NOLOCK)
WHERE failed_to_create_worker = 1
HAVING COUNT(*) > 0

DROP TABLE #Volumes;
DROP TABLE #Files;