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
2022-03-27 - Added -1 options for @InitialSizeMBOverride and @FileGrowthMB to align to the current max setting
2021-08-16 - Added @ForceAllowSameDiskWithOtherDBs, added support to force UNLIMITED using @MaxSizeMBOverride 
2021-07-12 - Added @MaxSizeMBOverride, fixed nvarchar conversions, fixed @InitialSizeMBOverride, added TF details
2020-10-11 - Added WITH NO_INFOMSGS to shrink command, added ForceShrink parameter
2020-08-04 - Added @ClearServerCache and @SpaceUsedMaxPercent parameters, updated indentations and comments
2020-03-08 - Bug fixes, added parameters for more control
2020-01-19 - Removed max size setting, replaced with UNLIMITED. Added @FileGrowthMB as parameter.
2019-11-19 - First version
-----------------------------------
*/

DECLARE @ClearServerCache 		BIT	= 0 -- If shrinking tempdb files doesn't work, you may have to set this to 1 to clear objects from server cache.
DECLARE @MaxSizeDiskUtilizationPercent 	FLOAT 	= NULL -- Maximum total percentage to be used for MAXSIZE property. Use NULL for UNLIMITED.
DECLARE @InitSizeDiskUtilizationPercent FLOAT 	= NULL -- Desired total percentage of disk space to be used for SIZE property.
DECLARE @InitialSizeMBOverride 		INT 	= 1024 -- hard-coded starting size per file for edge cases, overriding the calculation based on disk size. Set to -1 to align all files to the file with the maximum size.
DECLARE @MaxSizeMBOverride 		INT 	= -1 -- hard-coded max size per file for edge cases, overriding the calculation based on disk size. Set to -1 for UNLIMITED.
DECLARE @FileGrowthMB 			INT 	= 64 -- Auto-growth increment in MB. Set to -1 to align all files to the file with the maximum auto-growth setting.
DECLARE @IncludeTransactionLog 		BIT 	= 0 -- Set this to 1 to include the transaction log file in the calculations.
DECLARE @SpaceUsedMaxPercent 		INT 	= 50 -- Maximum percent space used compared to desired file size, if found to be above this - a warning will be raised.
DECLARE @ForceShrink			BIT	= 0 -- Set this to 1 to force shrink (use this if actual tempdb file size on disk is larger than what's specified in system tables).
DECLARE @ForceAllowSameDiskWithOtherDBs	BIT	= 1 -- Set this to 1 to allow for TempDB to be located on the same physical disk with other databases.
DECLARE @DoNotResize			BIT = 0	-- Set this to 1 to avoid ALTER DATABASE ... MODIFY FILE commands.

/** DO NOT CHANGE ANYTHING BELOW THIS LINE **/

SET NOCOUNT, XACT_ABORT, ARITHABORT ON;
USE [tempdb];

DECLARE @CMDs AS TABLE (CMD nvarchar(max) NULL);

IF @InitialSizeMBOverride = -1
BEGIN
	SELECT @InitialSizeMBOverride = MAX(size) / 128
	FROM tempdb.sys.database_files
	WHERE type = 0
END

IF @FileGrowthMB = -1
BEGIN
	SELECT @FileGrowthMB = MAX(growth) / 128
	FROM tempdb.sys.database_files
	WHERE type = 0 AND is_percent_growth = 0

	IF @FileGrowthMB = -1
	BEGIN
		SELECT @FileGrowthMB = (MAX(growth) / 100.0 * @InitialSizeMBOverride) / 128
		FROM tempdb.sys.database_files
		WHERE type = 0 AND is_percent_growth = 1
	END
END

INSERT INTO @CMDs
SELECT
 Remediation_Script = 
 CASE WHEN current_size_MB > InitSizeMBPerFile OR @ForceShrink = 1 THEN N'USE tempdb; DBCC SHRINKFILE (N' + QUOTENAME(name, '''') + ' , ' + CONVERT(nvarchar(max),InitSizeMBPerFile) + N') WITH NO_INFOMSGS; '
 ELSE N'' END
+ CASE WHEN @DoNotResize = 1 THEN N''
  ELSE '	ALTER DATABASE [tempdb] MODIFY FILE (NAME = ' + QUOTENAME(name, '''') + ', SIZE = ' + CONVERT(nvarchar(max),InitSizeMBPerFile) + N'MB, MAXSIZE = ' + ISNULL(CONVERT(nvarchar(max),MaxSizeMBPerFile) + 'MB','UNLIMITED') + N', FILEGROWTH = ' + CONVERT(nvarchar(max), @FileGrowthMB) + N'MB);'
  END
+ CONCAT('-- Current size: ', current_size_MB, ' MB, Space Used: ', current_spaceused_MB, ' MB (', ROUND(current_spaceused_MB * 1.0 / InitSizeMBPerFile * 100,2), ' % of desired size)')
+ CASE WHEN current_spaceused_MB * 1.0 / InitSizeMBPerFile > @SpaceUsedMaxPercent / 100.0 THEN ' !!WARNING!!' ELSE '' END
--, *
FROM
(
	SELECT 
		  MaxSizeMBPerFile = CASE WHEN @MaxSizeMBOverride = -1 THEN NULL ELSE ISNULL(@MaxSizeMBOverride, CEILING(@MaxSizeDiskUtilizationPercent / 100.0 * total_MB_ceil / TotalNumOfFiles)) END
		, InitSizeMBPerFile = ISNULL(@InitialSizeMBOverride, FLOOR(@InitSizeDiskUtilizationPercent / 100.0 * total_MB_floor / TotalNumOfFiles))
		, *
	FROM
	(
	select
	  B.total_bytes
	, current_size_MB = size * 8 / 1024
	, current_maxsize_MB = max_size * 8 / 1024
	, current_growth_MB = growth * 8 / 1024
	, current_spaceused_MB = FILEPROPERTY(A.[name], 'SpaceUsed') / 128
	, total_MB_ceil = CEILING(B.total_bytes / 1024 / 1024)
	, total_MB_floor = FLOOR(B.total_bytes / 1024 / 1024)
	, TotalNumOfFiles = COUNT(*) OVER ()
	, A.[name]
	, A.[type]
	, A.is_percent_growth
	, B.volume_mount_point
	from sys.master_files A
	CROSS APPLY sys.dm_os_volume_stats (A.database_id, A.[file_id]) B
	WHERE A.database_id = 2
	AND A.[type] IN (0,1)
	AND (@ForceAllowSameDiskWithOtherDBs = 1 OR NOT EXISTS
		(
		select null
		from sys.master_files A1
		CROSS APPLY sys.dm_os_volume_stats (A1.database_id, A1.[file_id]) B1
		WHERE A1.database_id <> 2
		AND B1.volume_mount_point = B.volume_mount_point
		))
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
	SELECT Remediation_Script = N'DBCC TRACEON(' + CONVERT(nvarchar(max),TF) + N', -1); -- Add to startup parameters: -T' + CONVERT(nvarchar(max),TF)
	FROM @TF WHERE Glob = 0

END

SELECT *
FROM (
VALUES
(N'CHECKPOINT;'),
(N'GO'),
(N'DBCC FREEPROCCACHE'),
(N'GO'),
(N'DBCC FREESYSTEMCACHE (''ALL'')'),
(N'GO')) AS v(Remediation_Script)
WHERE @ClearServerCache = 1
AND EXISTS (SELECT NULL FROM @CMDs WHERE CMD NOT LIKE 'DBCC TRACE%')
UNION ALL
SELECT CMD AS Remediation_Script FROM @CMDs

IF EXISTS (SELECT * FROM @CMDs WHERE CMD LIKE 'DBCC TRACEON(%')
	RAISERROR(N'WARNING! Detected missing trace flag(s). Please remember to add these trace flags to the server startup parameters as well!', 11,1);

IF EXISTS (SELECT * FROM @CMDs WHERE CMD LIKE '%DBCC SHRINKFILE%--% !!WARNING!!')
	RAISERROR(N'WARNING! One or more files have a high space used compared to the desired file size!
Please try to reduce TempDB workload, and/or increase the desired file size percentage.', 16,1);