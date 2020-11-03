IF OBJECT_ID('tempdb..#tmp') IS NOT NULL
DROP TABLE #tmp

CREATE TABLE #tmp(
	DBNAme SYSNAME,
	FileName VARCHAR(400),
	FileSize INT,
	AutogrowthValue INT
)

INSERT #tmp
SELECT 
DB_NAME(database_id) AS DBname,
[FileName] = name,
dbSizeInMB = size/128,
growth/128 AS Autogrowth_Value_InMB
FROM sys.master_files
WHERE database_id > 4 
AND type = 0  -- 1 LOG   0 DATA
ORDER BY 
--growth DESC,
size DESC


SELECT
CASE WHEN FileSize < 512   AND FileSize >= 1     THEN 'ALTER DATABASE ' + QUOTENAME(DBNAme) + ' MODIFY FILE (NAME = ''' + FileName + ''' ,filegrowth = 128MB)' 
	 WHEN FileSize <= 1024 AND FileSize >= 512  THEN 'ALTER DATABASE ' + QUOTENAME(DBNAme) + ' MODIFY FILE (NAME = ''' + FileName + ''' ,filegrowth = 512MB)' 
	 WHEN FileSize > 1024 AND FileSize < 2048   THEN 'ALTER DATABASE ' + QUOTENAME(DBNAme) + ' MODIFY FILE (NAME = ''' + FileName + ''' ,filegrowth = 1024MB)'
	 WHEN FileSize > 2048 AND FileSize < 5116   THEN 'ALTER DATABASE ' + QUOTENAME(DBNAme) + ' MODIFY FILE (NAME = ''' + FileName + ''' ,filegrowth = 2048MB)'
	 WHEN FileSize > 5116                       THEN 'ALTER DATABASE ' + QUOTENAME(DBNAme) + ' MODIFY FILE (NAME = ''' + FileName + ''' ,filegrowth = 5116MB)'
END
FROM #tmp





