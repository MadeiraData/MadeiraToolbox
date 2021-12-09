USE [master];
GO

DROP TABLE IF EXISTS #AllDatabases;
CREATE TABLE #AllDatabases
					(
						DBName NVARCHAR(128),
						Result NVARCHAR(256)
					);

INSERT #AllDatabases
EXEC sp_msforeachdb N'USE [?];
SELECT
	DB_NAME(),
	CASE
		WHEN EXISTS
					(
						SELECT 1
						FROM
							sys.sysusers
						WHERE
							[name] = ''guest''
							AND hasdbaccess = 1
					)
		THEN N''USE '' + N''[?]'' + N'';
GO

REVOKE CONNECT FROM GUEST''
				ELSE N''The guest user is properly configured on the database''
	END
WHERE
	DB_NAME() NOT IN (''master'', ''msdb'', ''tempdb'');

'

SELECT
	DBName,
	Result
FROM
	#AllDatabases
WHERE
	Result != N'The guest user is properly configured on the database';
GO
