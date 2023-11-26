/*
Author: Eitan Blumin
Date: 2023-11-21
Description:
Check the SQL Server Error Log and SQL Agent Error Log sizes.
If they're bigger than 50 MB, then they should be cycled.
*/

SET NOCOUNT ON;
DECLARE @Logs AS TABLE
(
 ArchiveNum SMALLINT,
 LastModified DATETIME,
 LogSizeBytes BIGINT
);

INSERT INTO @Logs
EXEC xp_enumerrorlogs 1

SELECT
N'In server: ' + QUOTENAME(@@SERVERNAME) + N' the ERROR LOG is too large. Please run sp_cycle_errorlog.' AS Msg
, LogSizeBytes / 1024 / 1024.0 AS LogSizeMB
FROM @Logs
WHERE ArchiveNum = 0
AND LogSizeBytes / 1024 / 1024.0 > 50
GO


SET NOCOUNT ON;
DECLARE @Logs AS TABLE
(
 ArchiveNum SMALLINT,
 LastModified DATETIME,
 LogSizeBytes BIGINT
);

INSERT INTO @Logs
EXEC xp_enumerrorlogs 2

SELECT
N'In server: ' + QUOTENAME(@@SERVERNAME) + N' the SQL Agent ERROR LOG is too large. Please run msdb.dbo.sp_cycle_agent_errorlog' AS Msg
, LogSizeBytes / 1024 / 1024.0 AS LogSizeMB
FROM @Logs
WHERE ArchiveNum = 0
AND LogSizeBytes / 1024 / 1024.0 > 50
