USE [master]
GO

-- This script search for all overlapped statistics and prepare DROP statement

IF OBJECT_ID('tempdb..#autostats')  IS NOT NULL
DROP TABLE #autostats

CREATE TABLE #autostats
						(
						[DataBase]				NVARCHAR(128),
						[Schema]				NVARCHAR(128),
						[Table]					NVARCHAR(128),
						[Column]				SYSNAME,
						[StatisticName]			NVARCHAR(128),
						[Overlapped by Index]	NVARCHAR(128),
						[ScriptToDropStitistic]	NVARCHAR(200)
						)

INSERT INTO #autostats
EXEC sp_MSforeachdb 'USE [?]

	SELECT
		DB_NAME(),
		OBJECT_SCHEMA_NAME(S.[object_id]),
		OBJECT_NAME(S.[object_id]),
		C.[name],
		A.[name],
		S.[name],
		''DROP STATISTICS ['' + OBJECT_SCHEMA_NAME(S.[object_id]) + ''].['' + OBJECT_NAME(S.[object_id]) + ''].[''+ A.[name] + '']''
	FROM
		sys.stats AS S
		INNER JOIN sys.stats_columns AS SC	ON S.[object_id] = SC.[object_id]
											AND S.stats_id = SC.stats_id
		INNER JOIN 
					(
						SELECT
							S.[object_id],
							S.[stats_id],
							S.[name],
							SC.[column_id]
						FROM
							sys.stats S
							INNER JOIN sys.stats_columns AS SC	ON S.[object_id] = SC.[object_id]
																AND S.stats_id = SC.stats_id
						WHERE
							S.auto_created = 1
							AND SC.stats_column_id = 1
					) A
						ON SC.[object_id] = A.[object_id]
						AND SC.column_id = A.column_id
		INNER JOIN sys.columns AS C	ON S.[object_id] = C.[object_id]
									AND SC.column_id = C.column_id
	WHERE
		S.auto_created = 0
		AND SC.stats_column_id = 1
		AND SC.stats_id != A.stats_id
		AND OBJECTPROPERTY(S.[object_id], ''IsMsShipped'') = 0 '

SELECT
	*
FROM
	#autostats