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
-- 1. Look in index usage stats for the most "popular" non-clustered indexes which would be a good candidate as clustered index. If no such was found, then:
-- 2. Look in missing index stats for the most impactful index that has the highest number of INCLUDE columns. If no such was found, then:
-- 3. If there's any non-clustered index at all, get the first one created with the highest number of INCLUDE columns, give priority to UNIQUE indexes. If no such was found, then:
-- 4. Use the IDENTITY column in the table. If no such was found, then:
-- 5. Use the first date/time column in the table, give priority to columns with a default constraint. If no such was found, then:
-- 6. Check for any column statistics in the table and look for the column which is the most selective (most unique values). If no such was found, then:
-- 7. Bummer. I'm out of ideas. No recommendations are possible.
-------------------------------------------------------
-- Change log:
-- ------------
-- 2020-02-19	Added support for Azure SQL DB, and added version-dependent check to ignore memory optimized tables
-- 2020-02-12	Changed prioritization a bit for the recommendations, added automatic generation of basic CREATE script
-- 2020-01-07	Added check of database Updateability, and moved around a few columns
-- 2019-12-29	Added checks for IDENTITY columns, and first DATE/TIME columns
-- 2019-12-23	First version
-------------------------------------------------------
-- Parameters:
-- ------------
DECLARE
	 @MinimumRowsInTable	INT		=	200000	-- Minimum number of rows in a table in order to check it
-------------------------------------------------------


SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
IF OBJECT_ID(N'tempdb..#temp_heap') IS NOT NULL DROP TABLE #temp_heap;

DECLARE @CMD NVARCHAR(MAX), @CurrDB SYSNAME, @CurrObjId INT, @CurrTable NVARCHAR(1000)

CREATE TABLE #temp_heap
    (
        [database_name] NVARCHAR(50),
        table_name NVARCHAR(MAX),
        full_table_name NVARCHAR(MAX),
	num_of_rows INT NULL,
	[object_id] INT,
        candidate_index SYSNAME NULL,
	candidate_columns_from_missing_index NVARCHAR(MAX) NULL,
	identity_column SYSNAME NULL,
	most_selective_column_from_stats SYSNAME NULL,
	first_date_column SYSNAME NULL,
	first_date_column_default NVARCHAR(MAX) NULL
    );

SET @CMD = N'
 INSERT INTO #temp_heap([database_name], [object_id], table_name, full_table_name, num_of_rows, candidate_index)
 SELECT DB_NAME() as DatabaseName, t.object_id, OBJECT_NAME(t.object_id) AS table_name, QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) + ''.'' + QUOTENAME(OBJECT_NAME(t.object_id)) AS FullTableName
 , SUM(p.rows)
 , QUOTENAME(ix.name) AS CandidateIndexName
 FROM sys.tables t
 INNER JOIN sys.partitions p
 ON t.object_id = p.OBJECT_ID
 OUTER APPLY
 (
	SELECT TOP 1 us.index_id
	FROM sys.dm_db_index_usage_stats AS us
	WHERE us.database_id = DB_ID()
	AND us.object_id = t.object_id
	AND us.index_id > 1
	ORDER BY us.user_updates DESC, us.user_scans DESC, us.user_seeks DESC
 ) AS ixus
 LEFT JOIN sys.indexes AS ix
 ON ixus.index_id = ix.index_id
 AND ix.object_id = t.object_id
 WHERE p.index_id = 0
 AND t.is_ms_shipped = 0
 AND t.OBJECT_ID > 255'
 -- Ignore memory-optimized tables in SQL Server versions 2014 and newer
 + CASE WHEN CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 12 THEN N'
 AND t.is_memory_optimized = 0'
 ELSE N'' END + N'
 GROUP BY t.object_id, ix.name
 ' + ISNULL(N'HAVING SUM(p.rows) >= ' + CONVERT(nvarchar,@MinimumRowsInTable), N'')

IF CONVERT(varchar(300),SERVERPROPERTY('Edition')) = 'SQL Azure'
BEGIN
	exec (@CMD)
END
ELSE
BEGIN
	SET @CMD =  N'
IF EXISTS (SELECT * FROM sys.databases WHERE database_id > 4 AND name = ''?'' AND state_desc = ''ONLINE'' AND DATABASEPROPERTYEX(name, ''Updateability'') = ''READ_WRITE'')
BEGIN
 USE [?];'
+ @CMD + N'
END'
	exec sp_MSforeachdb @CMD
END

-- Add recommendations based on missing index stats
UPDATE t
	SET candidate_columns_from_missing_index = mi.indexColumns
FROM #temp_heap AS t
 CROSS APPLY
 (
	SELECT TOP 1 ISNULL(mid.equality_columns, mid.inequality_columns) AS indexColumns
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
FAST_FORWARD READ_ONLY
FOR
SELECT database_name, object_id, full_table_name
FROM #temp_heap AS t
--WHERE t.candidate_columns_from_missing_index IS NULL AND t.candidate_index IS NULL -- filters for only those without existing recommendation

OPEN Tabs
FETCH NEXT FROM Tabs INTO @CurrDB, @CurrObjId, @CurrTable

WHILE @@FETCH_STATUS = 0
BEGIN
	-- Get additional metadata for current table
	DECLARE @FirstIndex SYSNAME, @IdentityColumn SYSNAME, @FirstDateColumn SYSNAME, @FirstDateColumnDefault NVARCHAR(MAX);
	SET @CMD = N'SELECT TOP 1 @FirstIndex = name FROM ' + QUOTENAME(@CurrDB) + N'.sys.indexes AS ix 
	OUTER APPLY (SELECT SUM(CASE WHEN is_included_column = 1 THEN 1 ELSE 0 END) AS included_columns, COUNT(*) AS indexed_columns 
			FROM ' + QUOTENAME(@CurrDB) + N'.sys.index_columns AS ic WHERE ic.object_id = ix.object_id AND ic.index_id = ix.index_id) AS st  
	WHERE object_id = @ObjId AND index_id > 0 ORDER BY ix.is_unique DESC, included_columns DESC, indexed_columns ASC, index_id ASC;
	
	SELECT @IdentityColumn = [name]
	FROM ' + QUOTENAME(@CurrDB) + N'.sys.identity_columns
	WHERE object_id = @ObjId;

	SELECT TOP 1 @FirstDateColumn = c.[name], @FirstDateColumnDefault = dc.[definition]
	FROM ' + QUOTENAME(@CurrDB) + N'.sys.columns AS c
	LEFT JOIN ' + QUOTENAME(@CurrDB) + N'.sys.default_constraints AS dc
	ON c.default_object_id = dc.object_id
	AND c.object_id = dc.parent_object_id
	WHERE c.object_id = @ObjId
	AND c.system_type_id IN
	(SELECT system_type_id FROM ' + QUOTENAME(@CurrDB) + N'.sys.types WHERE precision > 0 AND (name LIKE ''%date%'' OR name LIKE ''%time%''))
	ORDER BY CASE WHEN dc.[definition] IS NOT NULL THEN 0 ELSE 1 END ASC, c.column_id ASC;'

	PRINT @CMD;
	SET @FirstIndex = NULL;
	SET @IdentityColumn = NULL;
	SET @FirstDateColumn = NULL;
	SET @FirstDateColumnDefault = NULL
	EXEC sp_executesql @CMD
			, N'@ObjId INT, @FirstIndex SYSNAME OUTPUT, @IdentityColumn SYSNAME OUTPUT, @FirstDateColumn SYSNAME OUTPUT, @FirstDateColumnDefault NVARCHAR(MAX) OUTPUT'
			, @CurrObjId, @FirstIndex OUTPUT, @IdentityColumn OUTPUT, @FirstDateColumn OUTPUT, @FirstDateColumnDefault OUTPUT

	IF @FirstIndex IS NOT NULL
	BEGIN
		---------------------
		-- Add recommendations based on existing non-clustered indexes (even if no existing or missing index stats found)
		---------------------
		UPDATE #temp_heap SET candidate_index = QUOTENAME(@FirstIndex) --+ N' (no usage)'
		WHERE database_name = @CurrDB AND object_id = @CurrObjId
	END

	IF @IdentityColumn IS NOT NULL
	BEGIN
		---------------------
		-- Add recommendations based on identity column
		---------------------
		UPDATE #temp_heap SET identity_column = QUOTENAME(@IdentityColumn)
		WHERE database_name = @CurrDB AND object_id = @CurrObjId;
	END
	
	IF @FirstDateColumn IS NOT NULL
	BEGIN
		-- Add recommendation based on the first date/time column
		UPDATE #temp_heap SET first_date_column = QUOTENAME(@FirstDateColumn)
		WHERE database_name = @CurrDB AND object_id = @CurrObjId;
	END

	--IF @FirstIndex IS NULL -- Performs check only if no previous recommendations found
	BEGIN
		---------------------
		-- Get recommendations based on most selective column based on statistics
		---------------------
		-- Get list of table columns
		DECLARE @Columns AS TABLE (colName SYSNAME);
		SET @CMD = N'SELECT name FROM ' + QUOTENAME(@CurrDB) + N'.sys.columns WHERE object_id = @ObjId AND is_computed = 0'

		INSERT INTO @Columns
		EXEC sp_executesql @CMD, N'@ObjId INT', @CurrObjId

		-- Generate and run SHOW_STATISTICS command
		SET @CMD = N'USE ' + QUOTENAME(@CurrDB) + N';
	SET NOCOUNT ON;'
	
		SELECT @CMD = @CMD + N'
		BEGIN TRY
		DBCC SHOW_STATISTICS(' + QUOTENAME(@CurrTable, '"') COLLATE database_default + N', ' + QUOTENAME(colName) COLLATE database_default + N') WITH DENSITY_VECTOR, NO_INFOMSGS;
		END TRY
		BEGIN CATCH
			PRINT ERROR_MESSAGE()
		END CATCH'
		FROM @Columns

		DECLARE @DensityStats AS TABLE (AllDensity FLOAT, AvgLength FLOAT, Cols NVARCHAR(MAX));

		INSERT INTO @DensityStats
		EXEC(@CMD);

		IF @@ROWCOUNT > 0
		BEGIN
			-- Set most selective column
			UPDATE #temp_heap
				SET most_selective_column_from_stats = 
						(
							SELECT TOP 1 QUOTENAME(Cols)
							FROM @DensityStats
							ORDER BY AllDensity ASC, AvgLength ASC
						)
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
	  N', candidate INDEX: ' + t.candidate_index
	, N', candidate column(s) from MISSING INDEX stats: ' + t.candidate_columns_from_missing_index
	, N', IDENTITY column: ' + t.identity_column
	, N', first DATE/TIME column: ' + t.first_date_column
	, N', most SELECTIVE column: ' + t.most_selective_column_from_stats
	, N', NO RECOMMENDATION POSSIBLE')
, Script = N'USE ' + QUOTENAME(t.database_name) 
	+ CASE WHEN t.candidate_index IS NOT NULL THEN N'; -- Recreate as clustered index: ' + t.candidate_index 
	ELSE 
	N'; CREATE CLUSTERED INDEX IX_clust ON ' + t.full_table_name 
	+ N' ('
	+ COALESCE(
		t.candidate_columns_from_missing_index,
		t.identity_column,
		t.first_date_column,
		t.most_selective_column_from_stats
		)
	+ N');' 
	END
, *
FROM #temp_heap AS t


--DROP TABLE #temp_heap