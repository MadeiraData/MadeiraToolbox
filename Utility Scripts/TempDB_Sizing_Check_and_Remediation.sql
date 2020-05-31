/*
-----------------------------------
TempDB Sizing Check and Remediation
-----------------------------------
Author: Eitan Blumin | https://www.eitanblumin.com
Description: 
This script makes sure that all TempDB files are equally sized, based on a calculation that takes into consideration
the disk volume where the TempDB files are located.
This check only works when TempDB files are isolated from other databases and exist on their own dedicated volume.

Change log:
2020-03-08 - Bug fixes, added parameters for more control
2020-01-19 - Removed max size setting, replaced with UNLIMITED. Added @FileGrowthMB as parameter.
2019-11-19 - First version
-----------------------------------
*/

SET NOCOUNT, XACT_ABORT, ARITHABORT ON;
DECLARE @MaxSizeDiskUtilizationPercent FLOAT = 95 -- use NULL for UNLIMITED
DECLARE @InitSizeDiskUtilizationPercent FLOAT = 70
DECLARE @InitialSizeMBOverride INT = NULL -- 2048 -- hard-coded number override for edge cases
DECLARE @FileGrowthMB INT = 1024
DECLARE @IncludeTransactionLog BIT = 0
DECLARE @CMDs AS TABLE (CMD nvarchar(max));

INSERT INTO @CMDs
SELECT
 Remediation_Script = 
 CASE WHEN current_size_MB > InitSizeMBPerFile THEN N'USE tempdb; DBCC SHRINKFILE (N' + QUOTENAME(name, '''') + ' , ' + CONVERT(nvarchar,InitSizeMBPerFile) + N'); '
 ELSE N'' END
+ 'ALTER DATABASE tempdb MODIFY FILE (NAME = ' + QUOTENAME(name, '''') + ', SIZE = ' + CONVERT(nvarchar,ISNULL(@InitialSizeMBOverride, InitSizeMBPerFile)) + N'MB, MAXSIZE = ' + ISNULL(CONVERT(nvarchar,MaxSizeMBPerFile) + 'MB','UNLIMITED') + N', FILEGROWTH = ' + CONVERT(nvarchar, @FileGrowthMB) + N'MB);'
--, *
FROM
(
	SELECT 
		  CEILING(@MaxSizeDiskUtilizationPercent / 100.0 * total_MB_ceil / TotalNumOfFiles) AS MaxSizeMBPerFile
		 , FLOOR(@InitSizeDiskUtilizationPercent / 100.0 * total_MB_floor / TotalNumOfFiles) AS InitSizeMBPerFile
		, *
	FROM
	(
	select
	  B.total_bytes
	, current_size_MB = size * 8 / 1024
	, current_maxsize_MB = max_size * 8 / 1024
	, current_growth_MB = growth * 8 / 1024
	, CEILING(B.total_bytes / 1024 / 1024) AS total_MB_ceil
	, FLOOR(B.total_bytes / 1024 / 1024) AS total_MB_floor
	, COUNT(*) OVER () AS TotalNumOfFiles
	, A.[name]
	, A.[type]
	, A.is_percent_growth
	, B.volume_mount_point
	from sys.master_files A
	CROSS APPLY
		sys.dm_os_volume_stats (A.database_id, A.[file_id]) B
	WHERE A.database_id = 2
	AND A.[type] IN (0,1)
	AND NOT EXISTS
	(
		select null
		from sys.master_files A1
		CROSS APPLY
			sys.dm_os_volume_stats (A1.database_id, A1.[file_id]) B1
		WHERE A1.database_id <> 2
		AND B1.volume_mount_point = B.volume_mount_point
	)
	) AS f
) AS f2
WHERE
	(@IncludeTransactionLog = 1 OR [type] = 0)
AND
(
	is_percent_growth = 1
OR current_growth_MB <> @FileGrowthMB
OR current_size_MB <> InitSizeMBPerFile
OR current_maxsize_MB <> MaxSizeMBPerFile
)

-- If SQL version lower than 2016:
IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) < 13
BEGIN
	DECLARE @TF AS TABLE (TF INT, Stat TINYINT, Glob TINYINT, Sess TINYINT);

	INSERT INTO @TF
	EXEC ('DBCC TRACESTATUS(1117,1118) WITH NO_INFOMSGS');

	IF @@ROWCOUNT > 0
	INSERT INTO @CMDs
	SELECT
		Remediation_Script = N'DBCC TRACEON(' + CONVERT(nvarchar,TF) + N', -1);'
	FROM @TF WHERE Glob = 0

END


SELECT CMD AS Remediation_Script FROM @CMDs