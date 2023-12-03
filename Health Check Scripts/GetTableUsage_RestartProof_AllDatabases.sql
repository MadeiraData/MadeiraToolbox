/*
*******************************************************************
********Get Table Usage for all databases for MSSQL Server)********
*******************************************************************
Written by: Eric Rouach, Madeira Data Solutions
Date: November 2023

The solution below will provide a "server-restart-proof" table usage sample across all user databases

Contents:
[dbo].[IndexUsageStats] table creation
[dbo].[IndexUsageStatsSnap] table creation
[dbo].[MergeIndexUsageStats] stored procedure creation

Once the above 3 objects are created, you may whether execute the [dbo].[MergeIndexUsageStats] 
stored procedure manually or create a SQL Server Agent job to execute it according to your needs.
*/

--Create [dbo].[IndexUsageStats] table
IF OBJECT_ID('[dbo].[IndexUsageStats]') IS NOT NULL
DROP TABLE [dbo].[IndexUsageStats]
CREATE TABLE [dbo].[IndexUsageStats]
(
	[db_id] SMALLINT NOT NULL,
	[db_name] NVARCHAR(128) NOT NULL,
	[schema_name] NVARCHAR(128) NOT NULL,
	[table_name] SYSNAME NOT NULL,
	[rows_count] INT NOT NULL,
	[user_scans] BIGINT NOT NULL,
	[user_seeks] BIGINT NOT NULL,
	[user_lookups] BIGINT NOT NULL,
	[user_updates] BIGINT NOT NULL,
	[last_user_scan] DATETIME NULL,
	[last_user_seek] DATETIME NULL,
	[last_user_lookup] DATETIME NULL,
	[last_user_update] DATETIME NULL
) 
GO

--Create [dbo].[IndexUsageStatsSnap] table
IF OBJECT_ID('[dbo].[IndexUsageStatsSnap]') IS NOT NULL
DROP TABLE [dbo].[IndexUsageStatsSnap]
CREATE TABLE [dbo].[IndexUsageStatsSnap]
(
	[db_id] SMALLINT NOT NULL,
	[db_name] NVARCHAR(128) NOT NULL,
	[schema_name] NVARCHAR(128) NOT NULL,
	[table_name] SYSNAME NOT NULL,
	[rows_count] INT NOT NULL,
	[user_scans] BIGINT NOT NULL,
	[user_seeks] BIGINT NOT NULL,
	[user_lookups] BIGINT NOT NULL,
	[user_updates] BIGINT NOT NULL,
	[last_user_scan] DATETIME NULL,
	[last_user_seek] DATETIME NULL,
	[last_user_lookup] DATETIME NULL,
	[last_user_update] DATETIME NULL
) 
GO

--Create [dbo].[MergeIndexUsageStats] stored procedure
CREATE OR ALTER PROCEDURE [dbo].[MergeIndexUsageStats]
AS

BEGIN
	TRUNCATE TABLE [dbo].[IndexUsageStatsSnap]; --clean up the "snapshot" table 

	DECLARE @MyDatabase NVARCHAR(255) = 'IndexUsageStatsDB' --replace with the database inn which you created the above tables

	DECLARE @CurrentDB SYSNAME --declare cursor variable
	DECLARE @DynamicSQL NVARCHAR(MAX) = N'' --declare cursor variable

	DECLARE DB_Cursor CURSOR LOCAL FAST_FORWARD --declare cursor for executing dynamic sql against all databases
	FOR

		SELECT 
			[name]
		FROM 
			sys.databases
		WHERE 
			database_id > 4 --filter out system databases
			AND 
			DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE' --filter out read-only databases
			AND 
			HAS_DBACCESS([name]) = 1
			
	OPEN DB_Cursor
	FETCH NEXT FROM DB_Cursor INTO @CurrentDB
	
	WHILE @@FETCH_STATUS = 0  

	BEGIN
		SET @DynamicSQL += 
		N'
		USE '+QUOTENAME(@CurrentDB)+N'; 

		INSERT INTO	
			'+QUOTENAME(@MyDatabase)+N'.[dbo].[IndexUsageStatsSnap]
		(
			[db_id]
		,   [db_name]
		,   [schema_name]
		,	[table_name]
		,   [rows_count]
		,	[user_scans]
		,	[user_seeks]
		,	[user_lookups]
		,	[user_updates]
		,	[last_user_scan]
		,	[last_user_seek]
		,	[last_user_lookup]
		,	[last_user_update]
		)
		SELECT 
			DB_ID() AS [dbid]
		,   DB_NAME() AS [db_name]
		,	OBJECT_SCHEMA_NAME(i.object_id) AS [schema_name]
		,	OBJECT_NAME(i.[object_id], DB_ID()) AS [table_name]
		,   SUM(CASE
				WHEN i.index_id IN(0,1) THEN p.rows
				ELSE 0
				END) AS [rows_count]
		,	ISNULL(SUM(CASE WHEN p.partition_number = 1 THEN ddius.user_scans ELSE 0 END),0)
		,	ISNULL(SUM(CASE WHEN p.partition_number = 1 THEN ddius.user_seeks ELSE 0 END),0)
		,	ISNULL(SUM(CASE WHEN p.partition_number = 1 THEN ddius.user_lookups ELSE 0 END),0)
		,	ISNULL(SUM(CASE WHEN p.partition_number = 1 THEN ddius.user_updates ELSE 0 END),0)
		,	MAX(ddius.last_user_scan)
		,	MAX(ddius.last_user_seek)
		,	MAX(ddius.last_user_lookup)
		,	MAX(ddius.last_user_update)	
		FROM 
			sys.indexes i
		LEFT OUTER JOIN 
			sys.dm_db_index_usage_stats ddius
		ON 
			ddius.index_id = i.index_id
			AND 
			ddius.[object_id] = i.[object_id]
			AND 
			ddius.database_id = DB_ID()
		LEFT OUTER JOIN
			sys.partitions p
		ON
			i.[object_id] = p.[object_id]
		AND
			i.index_id = p.index_id
		WHERE 
			OBJECTPROPERTY(i.[object_id], ''IsUserTable'') = 1
			--AND i.index_id > 0 	-- filter out heaps
		GROUP BY
			OBJECT_SCHEMA_NAME(i.object_id)
		,	OBJECT_NAME(i.[object_id], DB_ID())
			;
		'
		EXEC sp_executesql @DynamicSQL
		FETCH NEXT FROM DB_Cursor INTO @CurrentDB
	END
	CLOSE DB_Cursor  
    DEALLOCATE DB_Cursor

	--At this point, the [dbo].[IndexUsageStatsSnap] table is full with data from all databases.

	--merge [dbo].[IndexUsageStats]
	MERGE [dbo].[IndexUsageStats] AS [t] --t stands for "target"
	USING 
	(
	SELECT DISTINCT
	       [db_id]
	      ,[db_name]
	      ,[schema_name]
	      ,[table_name]
		  ,[rows_count]
	      ,[user_scans]
	      ,[user_seeks]
	      ,[user_lookups]
	      ,[user_updates]
	      ,[last_user_scan]
	      ,[last_user_seek]
	      ,[last_user_lookup]
	      ,[last_user_update]
	FROM 
		   [dbo].[IndexUsageStatsSnap]
	) AS [s] --s stands for "source"
	ON 
		[t].[db_name] = [s].[db_name]
		AND
        [t].[schema_name] = [s].[schema_name]
		AND 
		[t].[table_name] = [s].[table_name]

	WHEN MATCHED 
		THEN UPDATE SET 
			[t].[user_scans] = 
				CASE WHEN (([s].[last_user_scan] > [t].[last_user_scan] OR [t].[last_user_scan] IS NULL) AND [s].[user_scans] > [t].[user_scans]) THEN [s].[user_scans] 
					 WHEN ([s].[last_user_scan] > [t].[last_user_scan] AND [s].[user_scans] <= [t].[user_scans]) THEN [t].[user_scans] + [s].[user_scans] 
					 ELSE [t].[user_scans] -- DO NOTHING
				END
		, 	[t].[user_seeks] = 
				CASE WHEN (([s].[last_user_seek] > [t].[last_user_seek] OR [t].[last_user_seek] IS NULL) AND [s].[user_seeks] > [t].[user_seeks]) THEN [s].[user_seeks] 
					 WHEN ([s].[last_user_seek] > [t].[last_user_seek] AND [s].[user_seeks] <= [t].[user_seeks]) THEN [t].[user_seeks] + [s].[user_seeks] 
					 ELSE [t].[user_seeks] -- DO NOTHING
				END
		, 	[t].[user_lookups] = 
				CASE WHEN (([s].[last_user_lookup] > [t].[last_user_lookup] OR [t].[last_user_lookup] IS NULL) AND [s].[user_lookups] > [t].[user_lookups]) THEN [s].[user_lookups] 
					 WHEN ([s].[last_user_lookup] > [t].[last_user_lookup] AND [s].[user_lookups] <= [t].[user_lookups]) THEN [t].[user_lookups] + [s].[user_lookups] 
					 ELSE [t].[user_lookups] -- DO NOTHING
				END
		, 	[t].[user_updates] = 
				CASE WHEN (([s].[last_user_update] > [t].[last_user_update] OR [t].[last_user_update] IS NULL) AND [s].[user_updates] > [t].[user_updates]) THEN [s].[user_updates] 
					 WHEN ([s].[last_user_update] > [t].[last_user_update] AND [s].[user_updates] <= [t].[user_updates]) THEN [t].[user_updates] + [s].[user_updates]
					 ELSE [t].[user_updates] -- DO NOTHING
				END
		, 	[t].[last_user_scan] = COALESCE([s].[last_user_scan], [t].[last_user_scan])
		, 	[t].[last_user_seek] = COALESCE([s].[last_user_seek], [t].[last_user_seek])
		, 	[t].[last_user_lookup] = COALESCE([s].[last_user_lookup], [t].[last_user_lookup])
		, 	[t].[last_user_update] = COALESCE([s].[last_user_update], [t].[last_user_update])
	WHEN NOT MATCHED BY TARGET
		THEN INSERT 
		(
		  [db_id]
	    , [db_name]
	    , [schema_name]
		, [table_name]
		, [rows_count]
		, [user_scans]
		, [user_seeks]
		, [user_lookups]
		, [user_updates]
		, [last_user_scan]
		, [last_user_seek]
		, [last_user_lookup]
		, [last_user_update]
		)
		VALUES
		(
		 [s].[db_id]
	    ,[s].[db_name]
	    ,[s].[schema_name]
		,[s].[table_name]
		,[s].[rows_count]
		,[s].[user_scans]
		,[s].[user_seeks]
		,[s].[user_lookups]
		,[s].[user_updates]
		,[s].[last_user_scan]
		,[s].[last_user_seek]
		,[s].[last_user_lookup]
		,[s].[last_user_update]
		)
	WHEN NOT MATCHED BY SOURCE
		THEN DELETE;
END
GO