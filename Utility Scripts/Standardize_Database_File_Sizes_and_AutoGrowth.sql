/*
Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
Date: 2020-01-28
Description:
This script generates commands to implement a minimal standardization of all database files in the instance.
List of implemented standards:
	1. Files must not be allowed to have percentage growth (defaults to 1GB growth instead as a replacement)
	2. Files must all have UNLIMITED max size
	3. Log files must be at least 64MB in size
	4. Log file auto-growth must be in power multiples of 2 between 64MB and 2048MB (i.e. 64,128,256,512,1024,2048) (defaults to 1GB growth instead as a replacement)
	5. Data file auto-growth, for data files bigger than 1GB, must be at least 100MB (defaults to 500MB growth instead as a replacement)

*/

SELECT *
, Remediation = N'ALTER DATABASE ' + QUOTENAME(DBName) + N' MODIFY FILE ( NAME = N' + QUOTENAME([name], N'''') + N', MAXSIZE = UNLIMITED'
			+ NewFileSize + NewFileGrowth + N');'
FROM
(
	SELECT *
	, NewFileSize = CASE
		WHEN [type_desc] = 'LOG' AND sizeMB < 64 THEN N', SIZE = 64MB'
		ELSE N''
		END
	, NewFileGrowth = CASE
		WHEN is_percent_growth = 1 THEN N', FILEGROWTH = 1024MB'
		WHEN [type_desc] = 'LOG' AND [growthMB] NOT IN (64,128,256,512,1024,2048) THEN N', FILEGROWTH = 1024MB'
		WHEN [type_desc] = 'ROWS' AND [growthMB] < 100 AND sizeMB > 1000 THEN N', FILEGROWTH = 500MB'
		ELSE N''
		END
	FROM
	(
		SELECT
			DB_NAME(database_id) AS DBName,
			[type_desc],
			[name],
			[size] / 128 AS sizeMB,
			NULLIF([max_size],-1) / 128 AS maxSizeMB,
			[is_percent_growth],
			[growthMB] = CASE [is_percent_growth] WHEN 0 THEN [growth]/128 END
		FROM sys.master_files
		WHERE database_id > 4
		AND type IN (0,1)
		AND state = 0
	) AS f
) AS f2
WHERE NewFileGrowth <> N''
OR NewFileSize <> N''
OR ISNULL(maxSizeMB,2097152) <> 2097152