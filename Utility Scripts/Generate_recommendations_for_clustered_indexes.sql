-------------------------------------------------------
------ Generate Clustered Index Recommendations -------
-------------------------------------------------------
-- Author: Eitan Blumin | https://www.eitanblumin.com
-- More info: https://eitanblumin.com/2019/12/30/resolving-tables-without-clustered-indexes-heaps/
-------------------------------------------------------
-- Description:
-- ------------
-- This script finds all heap tables, and "guestimates" a clustered index recommendation for each.
-- The script implements the following algorithm:
--
-- 1. Look in index usage stats for the most "popular" non-clustered indexes which would be a good candidate as clustered index, give priority to UNIQUE indexes. If no such was found, then:
-- 2. If there's any non-clustered index at all, get the first one created with the highest number of INCLUDE columns, give priority to UNIQUE indexes. If no such was found, then:
-- 3. Look in missing index stats for the most impactful index that has the highest number of INCLUDE columns. If no such was found, then:
-- 4. Use the IDENTITY column in the table. If no such was found, then:
-- 5. Check for any column statistics in the table and look for the column which is the most selective (most unique values). If no such was found, then:
-- 6. Use the first date/time column in the table, give priority to columns with a default constraint. If no such was found, then:
-- 7. Use the first int/bigint/smallint/tinyint column in the table, give priority to columns without a default constraint. If no such was found, then:
-- 8. Use the first non-nullable column in the table, give priority to columns without a default constraint. If no such was found, then:
-- 9. Bummer. I'm out of ideas. No recommendations are possible.
-------------------------------------------------------
-- Change log:
-- ------------
-- 2021-08-29	Changed INT to BIGINT to support larger tables
-- 2021-08-18	Added last read/write details per table
-- 2021-08-16	Fixed missing database context bug and some code quality issues.
-- 2021-04-18	Added enhancements for replacing primary key, and added parameter @DefaultClusteredIndexName
-- 2021-03-21	Fixed DROP command for unique or primary key constraints; added check for deprecated data types; some other minor fixes
-- 2021-02-15	Added details and logic based on index used pages count
-- 2020-11-25	Various improvements:
--			- Changed recommendations prioritization - gave higher priority to most SELECTIVE column
--			- Added parameter @RetainHighestCompression to retain DATA_COMPRESSION settings in scripts
--			- Ignore special index types (columnstore, XML, spatial, ...), and hypothetical indexes
--			- Give priority to UNIQUE indexes when prioritizing existing indexes based on usage stats
--			- Replaced usage of sp_MSforeachDb with a cursor, to support longer command text
-- 2020-11-18	Added Rollback_Script column in output
-- 2020-11-03	Added new step to find first integer column, and a new step to find first non-nullable column
-- 2020-09-30	Added optional parameters @OnlineRebuild, @SortInTempDB, @MaxDOP
-- 2020-09-21	Added columns list in initial recommendations retrieval, removed newlines from remediation scripts
-- 2020-07-14	Added proper support for replacing unique indexes
-- 2020-07-14	Added generated script for replacing existing nc index with a clustered index
-- 2020-02-19	Added support for Azure SQL DB, and added version-dependent check to ignore memory optimized tables
-- 2020-02-12	Changed prioritization a bit for the recommendations, added automatic generation of basic CREATE script
-- 2020-01-07	Added check of database Updateability, and moved around a few columns
-- 2019-12-29	Added checks for IDENTITY columns, and first DATE/TIME columns
-- 2019-12-23	First version
-------------------------------------------------------
-- Parameters:
-- ------------
DECLARE
	 @MinimumRowsInTable		BIGINT	= 200000	-- Minimum number of rows in a table in order to check it

	-- Parameters controlling the structure of output scripts:
	,@OnlineRebuild			BIT	= 1	-- If 1, will generate CREATE INDEX commands with the ONLINE option turned on.
	,@SortInTempDB			BIT	= 1	-- If 1, will generate CREATE INDEX commands with the SORT_IN_TEMPDB option turned on.
	,@MaxDOP			INT	= NULL	-- If not NULL, will generate CREATE INDEX commands with the MAXDOP option. Set to 1 to prevent parallelism and reduce workload.
	,@RetainHighestCompression	BIT	= 1	-- If 1, will retain the highest data compression setting when replacing existing indexes
	,@DefaultClusteredIndexName SYSNAME = N'IX_clust_{TableName}_{KeyColumns}' -- Default name for entirely new indexes. Supported placeholders: {TableName} , {KeyColumns}
-------------------------------------------------------


SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
IF OBJECT_ID(N'tempdb..#temp_heap') IS NOT NULL DROP TABLE #temp_heap;

DECLARE @CMD NVARCHAR(MAX), @CurrDB SYSNAME, @CurrObjId INT, @CurrTable NVARCHAR(1000);
DECLARE @RebuildOptions NVARCHAR(MAX);
DECLARE @spExecuteSql NVARCHAR(1000)

-- Init local variables and defaults
SET @RebuildOptions = N''
IF @OnlineRebuild = 1 SET @RebuildOptions = @RebuildOptions + N', ONLINE = {ONLINE_ON}'
IF @SortInTempDB = 1  SET @RebuildOptions = @RebuildOptions + N', SORT_IN_TEMPDB = ON'
IF @MaxDOP IS NOT NULL SET @RebuildOptions = @RebuildOptions + N', MAXDOP = ' + CONVERT(nvarchar(4000), @MaxDOP)
IF @RetainHighestCompression = 1 SET @RebuildOptions = @RebuildOptions + N', DATA_COMPRESSION = {COMPRESSION}'
IF @RebuildOptions LIKE N',%' SET @RebuildOptions = N' WITH (' + STUFF(@RebuildOptions, 1, 2, N'') + N')';

IF @OnlineRebuild = 1 AND ISNULL(CONVERT(int, SERVERPROPERTY('EngineEdition')),0) NOT IN (3,5,8)
BEGIN
	RAISERROR(N'-- WARNING: @OnlineRebuild is set to 1, but current SQL edition does not support ONLINE rebuilds.', 0, 1);
END

CREATE TABLE #temp_heap
    (
        [database_name] SYSNAME NOT NULL,
        table_name SYSNAME NOT NULL,
        full_table_name NVARCHAR(1000) NOT NULL,
	num_of_rows BIGINT NULL,
	heap_used_pages BIGINT NULL,
	[object_id] INT NULL,
        candidate_index SYSNAME NULL,
	candidate_index_used_pages BIGINT NULL,
	candidate_columns_from_existing_index NVARCHAR(MAX) NULL,
	include_columns_from_existing_index NVARCHAR(MAX) NULL,
	candidate_columns_from_missing_index NVARCHAR(MAX) NULL,
	identity_column SYSNAME NULL,
	most_selective_column_from_stats SYSNAME NULL,
	first_date_column SYSNAME NULL,
	first_integer_column SYSNAME NULL,
	first_integer_column_type SYSNAME NULL,
	first_non_nullable_column SYSNAME NULL,
	is_unique BIT NULL,
	is_primary_key BIT NULL,
	is_constraint BIT NULL,
	has_non_online_columns BIT NULL,
	data_compression_type TINYINT NULL,
	data_compression_type_desc AS (CASE data_compression_type WHEN 2 THEN 'PAGE' WHEN 1 THEN 'ROW' ELSE 'NONE' END)
    );

SET @CMD = N'
 SELECT DB_NAME() as DatabaseName, t.object_id, OBJECT_NAME(t.object_id) AS table_name, QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) + ''.'' + QUOTENAME(OBJECT_NAME(t.object_id)) AS FullTableName
 , p.total_rows
 , p.total_used_page_count
 , QUOTENAME(ix.name) AS CandidateIndexName
 , ix.total_used_page_count
 , ix_columns
 , inc_columns
 , ix.is_unique
 , ix.is_primary_key
 , ix.is_constraint
 , has_non_online_columns = CASE WHEN EXISTS (
		SELECT TOP 1 1 FROM sys.columns AS c
		WHERE c.object_id = t.object_id
		AND system_type_id IN (SELECT system_type_id FROM sys.types WHERE name IN (''image'',''text'',''ntext''))
		) THEN 1 ELSE 0 END
 , p.max_data_compression
 FROM sys.tables t
 CROSS APPLY
 (
	SELECT SUM(p.rows) AS total_rows, MAX(p.data_compression) AS max_data_compression, SUM(ps.used_page_count) AS total_used_page_count
	FROM sys.partitions AS p
	INNER JOIN sys.dm_db_partition_stats AS ps
	ON ps.index_id = p.index_id AND ps.object_id = p.object_id AND p.partition_id = ps.partition_id
	WHERE t.object_id = p.OBJECT_ID
	AND p.index_id = 0
	' + ISNULL(N'HAVING SUM(p.rows) >= ' + CONVERT(NVARCHAR(MAX),@MinimumRowsInTable), N'') + N'
 ) AS p
 OUTER APPLY
 (
	SELECT TOP 1 us.index_id, ix.[name], ix.is_unique, ix.is_primary_key, pstat.total_used_page_count
		, is_constraint = CASE WHEN 1 IN (ix.is_primary_key, ix.is_unique_constraint) THEN 1 ELSE 0 END
	FROM sys.dm_db_index_usage_stats AS us
	INNER JOIN sys.indexes AS ix
	ON us.index_id = ix.index_id AND us.object_id = ix.object_id
	CROSS APPLY
	(
		SELECT SUM(used_page_count) AS total_used_page_count
		FROM sys.dm_db_partition_stats AS ps
		WHERE ps.index_id = ix.index_id AND ps.object_id = ix.object_id
	) AS pstat
	WHERE us.database_id = DB_ID()
	AND us.object_id = t.object_id
	AND ix.index_id > 1
	AND ix.is_hypothetical = 0 AND ix.has_filter = 0
	AND ix.type <= 2
	ORDER BY CONVERT(tinyint, ix.is_unique) DESC, pstat.total_used_page_count ASC, us.user_updates DESC, us.user_scans DESC, us.user_seeks DESC
 ) AS ix
 OUTER APPLY
 (SELECT ix_columns = STUFF((
				SELECT '', '' + QUOTENAME(COL_NAME(ic.object_id, ic.column_id)) + CASE ic.is_descending_key WHEN 1 THEN '' DESC'' ELSE '' ASC'' END
				FROM sys.index_columns AS ic
				WHERE ic.object_id = t.object_id AND ic.index_id = ix.index_id AND ic.is_included_column = 0
				FOR XML PATH('''')
			), 1, 2, '''')
	, inc_columns = STUFF((
				SELECT '', '' + QUOTENAME(COL_NAME(ic.object_id, ic.column_id))
				FROM sys.index_columns AS ic
				WHERE ic.object_id = t.object_id AND ic.index_id = ix.index_id AND ic.is_included_column = 1
				FOR XML PATH('''')
			), 1, 2, '''')
) AS ixcolumns
 WHERE t.is_ms_shipped = 0
 AND t.OBJECT_ID > 255'
 -- Ignore memory-optimized tables in SQL Server versions 2014 and newer
 + CASE WHEN CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 12 THEN N'
 AND t.is_memory_optimized = 0'
 ELSE N'' END
 
IF CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5
BEGIN
	INSERT INTO #temp_heap([database_name], [object_id], table_name, full_table_name, num_of_rows, heap_used_pages, candidate_index, candidate_index_used_pages, candidate_columns_from_existing_index, include_columns_from_existing_index, is_unique, is_primary_key, is_constraint, has_non_online_columns, data_compression_type)
	exec (@CMD)
END
ELSE
BEGIN
	DECLARE DBs CURSOR
	LOCAL FAST_FORWARD
	FOR
	SELECT [name]
	FROM sys.databases
	WHERE database_id > 4 
	AND state_desc = 'ONLINE'
	AND DATABASEPROPERTYEX(name, 'Updateability') = 'READ_WRITE'

	OPEN DBs
	FETCH NEXT FROM DBs INTO @CurrDB

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @spExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

		INSERT INTO #temp_heap([database_name], [object_id], table_name, full_table_name, num_of_rows, heap_used_pages, candidate_index, candidate_index_used_pages, candidate_columns_from_existing_index, include_columns_from_existing_index, is_unique, is_primary_key, is_constraint, has_non_online_columns, data_compression_type)
		EXEC @spExecuteSql @CMD;

		FETCH NEXT FROM DBs INTO @CurrDB
	END

	CLOSE DBs
	DEALLOCATE DBs
END

-- Add recommendations based on missing index stats
UPDATE t
	SET candidate_columns_from_missing_index = mi.indexColumns
FROM #temp_heap AS t
 CROSS APPLY
 (
	SELECT TOP (1) ISNULL(mid.equality_columns, mid.inequality_columns) AS indexColumns
	FROM sys.dm_db_missing_index_group_stats AS migs  
	INNER JOIN sys.dm_db_missing_index_groups AS mig  
		ON (migs.group_handle = mig.index_group_handle)  
	INNER JOIN sys.dm_db_missing_index_details AS mid  
		ON (mig.index_handle = mid.index_handle)  
	WHERE mid.object_id = OBJECT_ID(QUOTENAME(t.[database_name]) + N'.' + t.full_table_name) AND mid.database_id = DB_ID(t.database_name)
	GROUP BY ISNULL(mid.equality_columns, mid.inequality_columns)
	ORDER BY MAX(LEN(mid.included_columns) - LEN(REPLACE(mid.included_columns, ', [', ''))) DESC
	, SUM(migs.avg_user_impact * migs.avg_total_user_cost) DESC
	, SUM(migs.user_scans) DESC, SUM(migs.user_seeks) DESC
 ) AS mi
--WHERE t.candidate_index IS NULL -- filters for only those without existing recommendation

DECLARE Tabs CURSOR
LOCAL FAST_FORWARD READ_ONLY
FOR
SELECT database_name, object_id, full_table_name
FROM #temp_heap AS t
--WHERE t.candidate_columns_from_missing_index IS NULL AND t.candidate_index IS NULL -- filters for only those without existing recommendation

OPEN Tabs
FETCH NEXT FROM Tabs INTO @CurrDB, @CurrObjId, @CurrTable

WHILE @@FETCH_STATUS = 0
BEGIN
	-- Get additional metadata for current table
	DECLARE @FirstIndex SYSNAME, @IsUnique BIT, @IsPK BIT, @IsConstraint BIT, @HasNonOnlineColumns BIT, @IdentityColumn SYSNAME
	, @FirstIndexColumns NVARCHAR(MAX), @FirstIndexIncludeColumns NVARCHAR(MAX)
	, @FirstDateColumn SYSNAME, @FirstIntColumn SYSNAME, @FirstIntColumnType SYSNAME, @FirstNonNullableColumn SYSNAME;
	SET @spExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql';

	SET @CMD = N'SELECT TOP 1 
		@FirstIndex = name,
		@IsUnique = ix.is_unique,
		@IsPK = ix.is_primary_key,
		@IsConstraint = CASE WHEN 1 IN (ix.is_primary_key, ix.is_unique_constraint) THEN 1 ELSE 0 END,
		@FirstIndexColumns = 
			STUFF((
				SELECT '', '' + QUOTENAME(COL_NAME(ic.object_id, ic.column_id)) + CASE ic.is_descending_key WHEN 1 THEN '' DESC'' ELSE '' ASC'' END
				FROM sys.index_columns AS ic
				WHERE ic.object_id = ix.object_id AND ic.index_id = ix.index_id AND ic.is_included_column = 0
				FOR XML PATH('''')
			), 1, 2, ''''),
		@FirstIndexIncludeColumns = 
			STUFF((
				SELECT '', '' + QUOTENAME(COL_NAME(ic.object_id, ic.column_id))
				FROM sys.index_columns AS ic
				WHERE ic.object_id = ix.object_id AND ic.index_id = ix.index_id AND ic.is_included_column = 1
				FOR XML PATH('''')
			), 1, 2, '''')
	FROM sys.indexes AS ix 
	OUTER APPLY (SELECT SUM(CASE WHEN is_included_column = 1 THEN 1 ELSE 0 END) AS included_columns, COUNT(*) AS indexed_columns 
			FROM sys.index_columns AS ic WHERE ic.object_id = ix.object_id AND ic.index_id = ix.index_id) AS st  
	WHERE object_id = @ObjId AND index_id > 0
	AND is_hypothetical = 0 AND ix.has_filter = 0
	AND type <= 2 -- ignore special index types
	ORDER BY ix.is_unique DESC, included_columns DESC, indexed_columns ASC, index_id ASC;
	
	SELECT @IdentityColumn = [name]
	FROM sys.identity_columns
	WHERE object_id = @ObjId;

	SELECT TOP 1 @FirstDateColumn = c.[name]
	FROM sys.columns AS c
	LEFT JOIN sys.default_constraints AS dc
	ON c.default_object_id = dc.object_id
	AND c.object_id = dc.parent_object_id
	WHERE c.object_id = @ObjId
	AND c.system_type_id IN
	(SELECT system_type_id FROM sys.types WHERE precision > 0 AND (name LIKE ''%date%'' OR name LIKE ''%time%''))
	ORDER BY
		CASE WHEN dc.[definition] IS NOT NULL THEN 0 ELSE 1 END ASC,
		CONVERT(smallint, c.is_nullable) ASC,
		c.column_id ASC;
	
	SELECT TOP 1 @FirstIntColumn = c.[name], @FirstIntColumnType = t.[name]
	FROM sys.columns AS c
	LEFT JOIN sys.default_constraints AS dc
	ON c.default_object_id = dc.object_id
	AND c.object_id = dc.parent_object_id
	LEFT JOIN sys.types AS t ON c.system_type_id = t.system_type_id
	WHERE c.object_id = @ObjId
	AND t.[name] IN (''bigint'', ''int'', ''smallint'', ''tinyint'')
	AND c.is_nullable = 0
	ORDER BY
		CASE WHEN dc.[definition] IS NOT NULL THEN 1 ELSE 0 END ASC,
		CASE t.[name] WHEN ''int'' THEN 1 WHEN ''bigint'' THEN 2 WHEN ''smallint'' THEN 3 ELSE 4 END ASC,
		c.column_id ASC;

	SELECT TOP 1 @FirstNonNullableColumn = c.[name]
	FROM sys.columns AS c
	LEFT JOIN sys.default_constraints AS dc
	ON c.default_object_id = dc.object_id
	AND c.object_id = dc.parent_object_id
	WHERE c.object_id = @ObjId
	AND c.is_nullable = 0
	ORDER BY
		CASE WHEN dc.[definition] IS NOT NULL THEN 1 ELSE 0 END ASC,
		c.column_id ASC;'

	PRINT @CMD;
	SET @FirstIndex = NULL;
	SET @IsUnique = NULL;
	SET @IsPK = NULL;
	SET @IsConstraint = NULL;
	SET @HasNonOnlineColumns = NULL;
	SET @IdentityColumn = NULL;
	SET @FirstDateColumn = NULL;
	SET @FirstIntColumn = NULL;
	SET @FirstIntColumnType = NULL;
	SET @FirstNonNullableColumn = NULL;
	EXEC @spExecuteSql @CMD
			, N'@ObjId INT, @FirstIndex SYSNAME OUTPUT, @IsUnique BIT OUTPUT, @IsPK BIT OUTPUT, @IsConstraint BIT OUTPUT, @HasNonOnlineColumns BIT OUTPUT, @FirstIndexColumns NVARCHAR(MAX) OUTPUT, @FirstIndexIncludeColumns NVARCHAR(MAX) OUTPUT, @IdentityColumn SYSNAME OUTPUT, @FirstDateColumn SYSNAME OUTPUT, @FirstIntColumn SYSNAME OUTPUT, @FirstIntColumnType SYSNAME OUTPUT, @FirstNonNullableColumn SYSNAME OUTPUT'
			, @CurrObjId, @FirstIndex OUTPUT, @IsUnique OUTPUT, @IsPK OUTPUT, @IsConstraint OUTPUT, @HasNonOnlineColumns OUTPUT, @FirstIndexColumns OUTPUT, @FirstIndexIncludeColumns OUTPUT, @IdentityColumn OUTPUT, @FirstDateColumn OUTPUT, @FirstIntColumn OUTPUT, @FirstIntColumnType OUTPUT, @FirstNonNullableColumn OUTPUT

	IF @FirstIndex IS NOT NULL
	BEGIN
		---------------------
		-- Add recommendations based on existing non-clustered indexes (even if no existing usage stats or missing index stats found)
		---------------------
		UPDATE #temp_heap SET candidate_index = QUOTENAME(@FirstIndex) --+ N' (no usage)'
		, candidate_columns_from_existing_index = @FirstIndexColumns
		, include_columns_from_existing_index = @FirstIndexIncludeColumns
		, is_unique = @IsUnique
		, is_primary_key = @IsPK
		, is_constraint = @IsConstraint
		WHERE database_name = @CurrDB AND object_id = @CurrObjId AND candidate_index IS NULL
	END

	IF @IdentityColumn IS NOT NULL
	BEGIN
		---------------------
		-- Add recommendations based on identity column
		---------------------
		UPDATE #temp_heap SET identity_column = QUOTENAME(@IdentityColumn)
		, is_unique = ISNULL(is_unique, 1)
		WHERE database_name = @CurrDB AND object_id = @CurrObjId;
	END
	
	IF @FirstDateColumn IS NOT NULL
	BEGIN
		-- Add recommendation based on the first date/time column
		UPDATE #temp_heap SET first_date_column = QUOTENAME(@FirstDateColumn)
		, is_unique = ISNULL(is_unique, 0)
		WHERE database_name = @CurrDB AND object_id = @CurrObjId;
	END
	
	IF @FirstIntColumn IS NOT NULL
	BEGIN
		-- Add recommendation based on the first integer column
		UPDATE #temp_heap SET first_integer_column = QUOTENAME(@FirstIntColumn)
		, first_integer_column_type = @FirstIntColumnType
		, is_unique = ISNULL(is_unique, 0)
		WHERE database_name = @CurrDB AND object_id = @CurrObjId;
	END

	IF @FirstNonNullableColumn IS NOT NULL
	BEGIN
		-- Add recommendation based on the first date/time column
		UPDATE #temp_heap SET first_non_nullable_column = QUOTENAME(@FirstNonNullableColumn)
		, is_unique = ISNULL(is_unique, 0)
		WHERE database_name = @CurrDB AND object_id = @CurrObjId;
	END

	--IF @FirstIndex IS NULL -- Performs check only if no previous recommendations found
	BEGIN
		---------------------
		-- Get recommendations based on most selective column based on statistics
		---------------------
		-- Get list of table columns
		DECLARE @Columns AS TABLE (colName SYSNAME NULL);
		SET @CMD = N'SELECT name FROM sys.columns WHERE object_id = @ObjId AND is_computed = 0'

		INSERT INTO @Columns
		EXEC @spExecuteSql @CMD, N'@ObjId INT', @CurrObjId;

		-- Generate and run SHOW_STATISTICS command
		SET @CMD = N'SET NOCOUNT ON;'
	
		SELECT @CMD = @CMD + N'
	BEGIN TRY
	DBCC SHOW_STATISTICS(' + QUOTENAME(@CurrTable, '"') COLLATE database_default + N', ' + QUOTENAME(colName) COLLATE database_default + N') WITH DENSITY_VECTOR, NO_INFOMSGS;
	END TRY
	BEGIN CATCH
		PRINT ERROR_MESSAGE()
	END CATCH'
		FROM @Columns

		DECLARE @DensityStats AS TABLE (AllDensity FLOAT NULL, AvgLength FLOAT NULL, Cols NVARCHAR(MAX) NULL);

		INSERT INTO @DensityStats
		EXEC @spExecuteSql @CMD;

		IF @@ROWCOUNT > 0
		BEGIN
			-- Set most selective column
			UPDATE #temp_heap
				SET most_selective_column_from_stats = 
						(
							SELECT TOP (1) QUOTENAME(Cols)
							FROM @DensityStats
							ORDER BY AllDensity ASC, AvgLength ASC
						)
				, is_unique = ISNULL(is_unique, 0)
			WHERE
				database_name = @CurrDB
			AND object_id = @CurrObjId;
		END
	END

	-- Re-init for next iteration
	DELETE @Columns;
	DELETE @DensityStats;

	FETCH NEXT FROM Tabs INTO @CurrDB, @CurrObjId, @CurrTable
END
CLOSE Tabs
DEALLOCATE Tabs


-- Output results
SELECT 
Details = 'Database:' +  QUOTENAME([database_name]) + ', Heap Table:' + full_table_name
+ COALESCE(
	  N', candidate INDEX: ' + t.candidate_index + ISNULL(N' (' + t.candidate_columns_from_existing_index + N')', N'')
	, N', candidate column(s) from MISSING INDEX stats: ' + t.candidate_columns_from_missing_index
	, N', IDENTITY column: ' + t.identity_column
	, N', most SELECTIVE column: ' + t.most_selective_column_from_stats
	, N', first DATE/TIME column: ' + t.first_date_column
	, N', first ' + ISNULL(UPPER(t.first_integer_column_type), 'INTEGER') + ' column: ' + t.first_integer_column
	, N', first non-nullable column: ' + t.first_non_nullable_column
	, N', NO RECOMMENDATION POSSIBLE' + CASE WHEN ixstat.last_write_dt IS NULL AND ixstat.last_read_dt IS NULL THEN N' (likely not in use)' ELSE N'' END
	)
+ CASE WHEN @OnlineRebuild = 1 AND t.has_non_online_columns = 1 THEN N'. !!! WARNING !!! ONLINE=ON not possible due to deprecated data types!' ELSE N'' END
, Script = N'USE ' + QUOTENAME(t.database_name) 
	+
	CASE 
	WHEN t.candidate_index IS NOT NULL AND t.candidate_columns_from_existing_index IS NULL THEN N'; -- Recreate as clustered index: ' + t.candidate_index
	WHEN t.candidate_index IS NOT NULL AND t.candidate_columns_from_existing_index IS NOT NULL THEN
		CASE WHEN t.is_constraint = 1
			THEN N'; ALTER TABLE ' + t.full_table_name + N' DROP CONSTRAINT ' + t.candidate_index
			ELSE N'; DROP INDEX ' + t.candidate_index + ' ON ' + t.full_table_name
		END
	+ N'; '
	+ CASE WHEN t.is_constraint = 1
			THEN N'ALTER TABLE ' + t.full_table_name + N' ADD CONSTRAINT ' + t.candidate_index
				+ CASE WHEN t.is_primary_key = 1 THEN N' PRIMARY KEY ' WHEN t.is_unique = 1 THEN N' UNIQUE ' ELSE N'' END
			ELSE N'CREATE ' + CASE WHEN t.is_unique = 1 THEN 'UNIQUE ' ELSE N'' END + N'CLUSTERED INDEX ' + t.candidate_index + ' ON ' + t.full_table_name 
		END
	+ N' (' + t.candidate_columns_from_existing_index + N')' + REPLACE(REPLACE(@RebuildOptions, N'{COMPRESSION}', t.data_compression_type_desc), '{ONLINE_ON}', CASE WHEN t.has_non_online_columns = 1 THEN 'OFF' ELSE 'ON' END)
	ELSE 
	N'; CREATE ' + CASE WHEN t.is_unique = 1 THEN 'UNIQUE ' ELSE N'' END + N'CLUSTERED INDEX ' + QUOTENAME(NewClusteredIndexName) + N' ON ' + t.full_table_name 
	+ N' ('
	+ keycolumns.KeyColumnsList
	+ N')' + REPLACE(REPLACE(@RebuildOptions, N'{COMPRESSION}', t.data_compression_type_desc), '{ONLINE_ON}', CASE WHEN t.has_non_online_columns = 1 THEN 'OFF' ELSE 'ON' END)
	END
, Rollback_Script = N'USE ' + QUOTENAME(t.database_name) 
	+
	CASE 
	WHEN t.candidate_index IS NOT NULL AND t.candidate_columns_from_existing_index IS NULL THEN N'; -- Recreate as nonclustered index: ' + t.candidate_index
	WHEN t.candidate_index IS NOT NULL AND t.candidate_columns_from_existing_index IS NOT NULL THEN N'; DROP INDEX ' + t.candidate_index + ' ON ' + t.full_table_name
	+ N'; CREATE ' + CASE WHEN t.is_unique = 1 THEN 'UNIQUE ' ELSE N'' END + N'NONCLUSTERED INDEX ' + t.candidate_index + ' ON ' + t.full_table_name 
	+ N' (' + t.candidate_columns_from_existing_index + N')' + ISNULL(N' INCLUDE (' + t.include_columns_from_existing_index + N')', N'')
	+ REPLACE(REPLACE(@RebuildOptions, N'{COMPRESSION}', t.data_compression_type_desc), '{ONLINE_ON}', CASE WHEN t.has_non_online_columns = 1 THEN 'OFF' ELSE 'ON' END)
	ELSE 
	N'; DROP INDEX ' + QUOTENAME(NewClusteredIndexName) + N' ON ' + t.full_table_name
	END
, *
FROM #temp_heap AS t
OUTER APPLY
(
SELECT KeyColumnsList = COALESCE(
		t.candidate_columns_from_missing_index,
		t.identity_column,
		t.most_selective_column_from_stats,
		t.first_date_column,
		t.first_integer_column,
		t.first_non_nullable_column
		)
) AS keycolumns
OUTER APPLY
(
SELECT NewClusteredIndexName =
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	@DefaultClusteredIndexName
	,N'{TableName}', t.table_name)
	,N'{KeyColumns}', keycolumns.KeyColumnsList)
	,N' ', N'_')
	,N',', N'')
	,N']', N'')
	,N'[', N'')
) AS ixname
OUTER APPLY
(
	SELECT max(stat.last_write_dt), max(stat.last_read_dt)
	FROM sys.dm_db_index_usage_stats AS us
	CROSS APPLY
	(VALUES
	(us.last_system_update, us.last_system_lookup),
	(us.last_user_update, us.last_system_scan),
	(NULL, us.last_system_seek),
	(NULL, us.last_user_lookup),
	(NULL, us.last_user_scan),
	(NULL, us.last_user_seek)
	) AS stat(last_write_dt, last_read_dt)
	WHERE us.database_id = DB_ID(t.database_name)
	AND us.object_id = t.object_id
) AS ixstat(last_write_dt, last_read_dt)

--DROP TABLE #temp_heap