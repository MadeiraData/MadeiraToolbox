/*
Copyright 2020 @EitanBlumin, https://eitanblumin.com

Source: https://bit.ly/TempDBFreeSpace
Full URL: https://gist.github.com/EitanBlumin/afed2587e89e260698c4753fcc5d1917

License: MIT (https://opensource.org/licenses/MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
CREATE OR ALTER PROCEDURE GetTempDBAvailableSpace
	@AvailableTempDBSpaceMB INT OUTPUT,
	@IncludeTransactionLog BIT = 0,
	@IncludeFreeDiskSpaceForAutoGrowth BIT = 1,
	@Verbose BIT = 0
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @CMD NVARCHAR(MAX)

	IF CONVERT(varchar(300),SERVERPROPERTY('Edition')) = 'SQL Azure'
	BEGIN
		RAISERROR(N'This procedure is not supported on SQL Azure databases.',16,1);
		RETURN -1;
	END

	-- Check free space remaining for TempDB
	SET @CMD = N'USE tempdb;
SELECT @AvailableTempDBSpaceMB = SUM(ISNULL(available_space_mb,0)) FROM
(
-- Get available space inside data files
SELECT 
  vs.volume_mount_point
, available_space_mb = SUM(ISNULL(f.size - FILEPROPERTY(f.[name], ''SpaceUsed''),0)) / 128 
' + CASE WHEN @IncludeFreeDiskSpaceForAutoGrowth = 1 THEN N'
			+ SUM(CASE
				-- If auto growth is disabled
				WHEN f.max_size = 0 THEN 0
				-- If remaining growth size till max size is smaller than remaining disk space, use remaining growth size till max size
				WHEN f.max_size > 0 AND (f.max_size - f.size) / 128 < (vs.available_bytes / 1024 / 1024) THEN (f.max_size - f.size) / 128
				-- Else, do not count available growth for this file
				ELSE 0
			END)' ELSE N'' END + N'
FROM sys.master_files AS f
CROSS APPLY sys.dm_os_volume_stats (f.database_id, f.file_id)  AS vs
WHERE f.database_id = 2
AND f.type' + CASE WHEN @IncludeTransactionLog = 0 THEN N' = 0' ELSE N' IN (0,1)' END + N'
GROUP BY vs.volume_mount_point
' + CASE WHEN @IncludeFreeDiskSpaceForAutoGrowth = 1 THEN N'
UNION ALL

-- Get available space on disk for auto-growth
SELECT 
  vs.volume_mount_point
, available_space_mb = vs.available_bytes / 1024 / 1024
FROM sys.master_files AS f
CROSS APPLY sys.dm_os_volume_stats (f.database_id, f.file_id)  AS vs
WHERE f.database_id = 2
AND f.type' + CASE WHEN @IncludeTransactionLog = 0 THEN N' = 0' ELSE N' IN (0,1)' END + N'
-- If max size is unlimited, or difference between current size and max size is bigger than available disk space
AND (f.max_size = -1 OR (f.max_size > 0 AND (f.max_size - f.size) / 128 > (vs.available_bytes / 1024 / 1024)))
GROUP BY vs.volume_mount_point, vs.available_bytes
' ELSE N'' END + N'
) AS q OPTION (RECOMPILE);'

	IF @Verbose = 1 PRINT @CMD;
	EXEC sp_executesql @CMD, N'@AvailableTempDBSpaceMB INT OUTPUT', @AvailableTempDBSpaceMB OUTPUT
	
	IF @Verbose = 1
	BEGIN
		SET @CMD = N'DECLARE @FileDetails NVARCHAR(MAX), @AggregatedDetails NVARCHAR(MAX)

USE tempdb;

-- Get available space inside data files
SELECT
@FileDetails = CONCAT(
ISNULL(@FileDetails + CHAR(10), N'''')
,QUOTENAME(f.[name])
,N'' size: ''
,f.size / 128
,N'' MB, max size: ''
,CASE f.max_size WHEN -1 THEN N''UNLIMITED'' WHEN 0 THEN ''DISABLED'' ELSE CONCAT(f.max_size / 128, N'' MB'') END
,N'', free space in file: ''
,ISNULL(f.size - FILEPROPERTY(f.[name], ''SpaceUsed''),0) / 128
,N'' MB, volume: ''
,vs.volume_mount_point
,N'' (''
,vs.available_bytes / 1024 / 1024
,N'' MB available)''
),
@AggregatedDetails =
CONCAT(
N''Total: ''
,SUM(vs.available_bytes / 1024 / 1024) OVER()
,N'' MB available on disk, ''
,SUM(ISNULL(f.size - FILEPROPERTY(f.[name], ''SpaceUsed''),0)) OVER() / 128
,N'' MB free space in files, ''
,SUM(CASE
		-- If auto growth is disabled
		WHEN f.max_size = 0 THEN 0
		-- If remaining growth size till max size is smaller than remaining disk space, use remaining growth size till max size
		WHEN f.max_size > 0 AND (f.max_size - f.size) / 128 < (vs.available_bytes / 1024 / 1024) THEN (f.max_size - f.size) / 128
		-- Else, do not count available growth for this file
		ELSE 0
	END) OVER()
,N'' MB available for growth''
)
FROM sys.master_files AS f
CROSS APPLY sys.dm_os_volume_stats (f.database_id, f.file_id)  AS vs
WHERE f.database_id = 2
AND f.type' + CASE WHEN @IncludeTransactionLog = 0 THEN N' = 0' ELSE N' IN (0,1)' END + N'

PRINT N''/*
'' + REPLICATE(N''='',LEN(@AggregatedDetails)+3)
PRINT @FileDetails
PRINT REPLICATE(N''='',LEN(@AggregatedDetails)+3)
PRINT @AggregatedDetails
PRINT REPLICATE(N''='',LEN(@AggregatedDetails)+3) + N''
*/'''
		EXEC sp_executesql @CMD
	END

	RETURN;
END