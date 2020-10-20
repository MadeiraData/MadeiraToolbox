/**************************************************************************
	Check Index Fragmentation based on Page Fullness and Fill Factor
***************************************************************************
Author: Eitan Blumin | https://www.eitanblumin.com
Version History:
	2020-10-20	Added @MaxDOP parameter, and better comments & indentation
	2020-01-07	First version

Description:
	This script was inspired by Erik Darling's blog post here:
https://www.erikdarlingdata.com/2019/10/because-your-index-maintenance-script-is-measuring-the-wrong-thing/

	!!! THIS SCRIPT MUST BE RUN IN THE CONTEXT OF THE DATABASE TO CHECK !!!
	
				-----------------
				!!!  WARNING  !!!
				-----------------
		This script uses "SAMPLED" mode for checking fragmentation,
		which can potentially cause significant IO stress on a
		large production server.
		Use at your own risk!
**************************************************************************/
DECLARE
	-- Parameters to limit which tables/indexes to check:
	 @MinPageCount					INT = 1000
	,@MinUserUpdates				INT = 1000

	-- Parameters to control when to change the FILLFACTOR:
	,@MinFragmentationToReduceFillFactor100		INT = 50
	,@MaxFragmentationToSetFillFactor100		INT = 20

	-- Parameters to control when to recommend REBUILD commands:
	,@MaxSpaceUsedForFillFactor100			INT = 90
	,@MaxSpaceUsedForFillFactorLessThan100		INT = 75

	-- Parameters to control settings of remediation (REBUILD) commands:
	,@OnlineRebuild					BIT = 0
	,@SortInTempDB					BIT = 0
	,@MaxDOP					TINYINT = NULL -- change to a value to limit DOP (e.g. MAXDOP=1 to disable parallelism)

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT, ARITHABORT, XACT_ABORT ON;

IF @OnlineRebuild = 1 AND ISNULL(CONVERT(int, SERVERPROPERTY('EngineEdition')),0) NOT IN (3,5,8)
BEGIN
	RAISERROR(N'Online Rebuild is not supported in this SQL Server edition.',16,1);
	GOTO Quit;
END

DECLARE @CommandTemplate NVARCHAR(MAX)
SET @CommandTemplate = N'RAISERROR(N''{DATABASE}.{TABLE} - {INDEX}'',0,1) WITH NOWAIT;
ALTER INDEX {INDEX} ON {TABLE}
REBUILD WITH(SORT_IN_TEMPDB=' 
+ CASE WHEN @SortInTempDB = 1 THEN N'ON' ELSE N'OFF' END 
+ N', ONLINE=' + CASE WHEN @OnlineRebuild = 1 THEN N'ON' ELSE N'OFF' END
+ CASE WHEN @MaxDOP IS NOT NULL THEN N', MAXDOP=' + CONVERT(nvarchar,@MaxDOP) ELSE N'' END
+ N'{FILLFACTOR});
GO'

SELECT
  DatabaseName = DB_NAME()
, SchemaName = OBJECT_SCHEMA_NAME(t.object_id)
, TableName = t.name
, IndexName = ix.name
, Remediation =
	REPLACE(REPLACE(REPLACE(REPLACE(@CommandTemplate
	, N'{DATABASE}', QUOTENAME(DB_NAME()))
	, N'{TABLE}', QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) + N'.' + QUOTENAME(t.name))
	, N'{INDEX}', QUOTENAME(ix.name))
	, N'{FILLFACTOR}', 
	CASE
		WHEN ix.fill_factor = 0 AND ps.avg_fragmentation_in_percent >= @MinFragmentationToReduceFillFactor100 
			THEN N', FILLFACTOR=90' 
		WHEN ix.fill_factor > 0 AND ps.avg_fragmentation_in_percent <= @MaxFragmentationToSetFillFactor100 
			THEN N', FILLFACTOR=100' 
	ELSE N''
	END)
, ix.fill_factor
, RowsCount = (SELECT SUM(rows) FROM sys.partitions AS p WHERE p.object_id = t.object_id AND p.index_id = ix.index_id)
, us.user_updates
, us.last_user_update
, ps.avg_fragmentation_in_percent
, ps.avg_page_space_used_in_percent
, ps.record_count
, ps.page_count
, ps.compressed_page_count
, t.object_id
, ix.index_id
, ps.partition_number
FROM
	sys.dm_db_index_usage_stats AS us
INNER JOIN
	sys.tables AS t ON us.object_id = t.object_id
INNER JOIN
	sys.indexes AS ix ON ix.object_id = t.object_id AND ix.index_id = us.index_id
CROSS APPLY
	sys.dm_db_index_physical_stats(DB_ID(), t.object_id, ix.index_id, NULL, 'SAMPLED') AS ps
WHERE
    us.database_id = DB_ID()
AND ps.alloc_unit_type_desc = 'IN_ROW_DATA'
AND t.is_ms_shipped = 0
AND us.user_updates >= @MinUserUpdates
AND ps.page_count >= @MinPageCount
AND 
(
	(
		ix.fill_factor <> 0
		AND ps.avg_page_space_used_in_percent <= @MaxSpaceUsedForFillFactorLessThan100
	)
	OR
	(
		ix.fill_factor = 0
		AND ps.avg_page_space_used_in_percent <= @MaxSpaceUsedForFillFactor100
	)
)
OPTION(MAXDOP 1);

Quit:
