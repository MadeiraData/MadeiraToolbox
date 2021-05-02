SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @Results AS TABLE
(
 DBName SYSNAME,
 SchemaName SYSNAME,
 TableName SYSNAME,
 IndexName SYSNAME,
 PercentComplete DECIMAL(5,2) NULL,
 PausedTime DATETIME NULL,
 AllocatedMB FLOAT NULL,
 TimeToAutoAbort INT NULL
);

INSERT INTO @Results
EXEC sp_MSforeachdb N'IF EXISTS (SELECT * FROM sys.databases WHERE name = ''?'' AND state = 0 AND DATABASEPROPERTYEX([name], ''Updateability'') = ''READ_WRITE'')
AND OBJECT_ID(QUOTENAME(''?'') + ''.sys.index_resumable_operations'') IS NOT NULL
BEGIN
USE [?];
SELECT
  DB_NAME() AS database_name,
  OBJECT_SCHEMA_NAME(iro.object_id) AS schema_name,
  OBJECT_NAME(iro.object_id) AS object_name,
  iro.name AS index_name,
  iro.percent_complete,
  iro.last_pause_time,
  iro.page_count / 128.0 AS index_operation_allocated_space_mb,
  IIF(CAST(dsc.value AS int) = 0, NULL, DATEDIFF(minute, CURRENT_TIMESTAMP, DATEADD(minute, CAST(dsc.value AS int), iro.last_pause_time))) AS time_to_auto_abort_minutes
FROM sys.index_resumable_operations AS iro
OUTER APPLY
(SELECT * FROM sys.database_scoped_configurations WHERE name = ''PAUSED_RESUMABLE_INDEX_ABORT_DURATION_MINUTES'')  AS dsc
WHERE iro.state_desc = ''PAUSED''
END
'

SELECT
  Details = CONCAT(N'In server ', @@SERVERNAME, N', database: ', QUOTENAME(DBName), N', table: '
 , QUOTENAME(SchemaName), N'.', QUOTENAME(TableName), N', resumable index ', QUOTENAME(IndexName), N' is paused at '
 , FORMAT(PercentComplete, '#,0.00'), N'% (', FORMAT(AllocatedMB, '#,0.00'), N' MB allocated) since ', CONVERT(nvarchar(19), PausedTime, 120)
 , ISNULL(N' time left till automatic abort: ' + CONVERT(nvarchar, TimeToAutoAbort) + N' minutes', N'')
 ),
 PauseDurationMinutes = DATEDIFF(minute, PausedTime, CURRENT_TIMESTAMP)
FROM @Results