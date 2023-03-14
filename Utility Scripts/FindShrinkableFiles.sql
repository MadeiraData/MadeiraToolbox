/*
Author: Eric Rouach, Madeira Data Solutions
Date: 2023-02-10
Title: Get top SQL Server DATA files with unused space.
Description: Having your DATA drive running out of space is usually an emergency situation. 
The script below will help you doing the following:
-find the SQL Server DATA OR LOG files with the greatest amount of releasable space.
-generate a remediation command for shrinking the files to the minimal possible size.

Some parts of this script are based on Ken Simmons' script from the blog post below:
https://www.mssqltips.com/sqlservertip/1510/script-to-determine-free-space-to-support-shrinking-sql-server-database-files/

====THIS IS AN EMERGENCY SOLUTION ONLY!!!====
====YOU MUST HAVE A "Database Files Maintenance Solution!!!====

Once you have identified the relevant files, you may, if needed, refer to this script 
for shrinking the file/s in specified increments:
https://github.com/MadeiraData/MadeiraToolbox/blob/413aa127f44ccd9b79636a3fd93cfeef66fae68c/Utility%20Scripts/Shrink_Database_File_in_Specified_Increments.sql
*/


USE master
GO 

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

--======ADJUST VALUES FOR THE 3 FOLLOWING VARIABLES======
DECLARE @Drive NCHAR(1) = 'C' -- replace this valus with the relevant Disk (e.g. 'G' or 'L'...)
DECLARE @MinimumPercentOfFreeSpace INT = 50 -- replace this value with the relevant % of free space you are looking for
DECLARE @FileType NCHAR(4) = 'LOG' -- replace with the file type you need: LOG or ROWS (data)
--======DO NOT CHANGE ANYTHING BELOW THIS LINE======


DROP TABLE IF EXISTS #FixedDrives
CREATE TABLE #FixedDrives 
	( 
		drive  CHAR(1), 
		[MB free] INT
	) 
INSERT INTO #FixedDrives 
EXEC xp_fixeddrives 

DROP TABLE IF EXISTS #SpaceUsed
CREATE TABLE #SpaceUsed 
	( 
		DbName    VARCHAR(50), 
		FileName  VARCHAR(50), 
		SpaceUsed FLOAT
	) 

DECLARE @Command NVARCHAR(MAX)
SET @Command = 
'
IF HAS_DBACCESS(''?'') = 0
	BEGIN
		RETURN
	END
ELSE
	BEGIN
		USE [?] 
		SELECT 
			''?'' DbName, 
			name FileName, 
			fileproperty(Name,''SpaceUsed'') as SpaceUsed  
		FROM 
			sys.sysfiles
	END
'
INSERT INTO #SpaceUsed 
EXEC sp_MSforeachdb @Command
;
WITH AllResults AS
(
SELECT   
	fd.drive, 
    CASE  
    WHEN (fd.[MB free]) > 1000 THEN CAST(CAST(((fd.[MB free]) / 1024.0) AS DECIMAL(18,2)) AS VARCHAR(20))+' GB' 
    ELSE CAST(CAST((fd.[MB free]) AS DECIMAL(18,2)) AS VARCHAR(20))+' MB' 
    END AS DiskFreeSpace,
	d.database_id AS DatabaseId,
    d.name AS DatabaseName, 
    mf.name AS [Filename], 
    CASE mf.type  
    WHEN 0 THEN 'DATA' 
    ELSE [type_desc] 
    END AS FileType, 
    CASE  
    WHEN (mf.size * 8 / 1024.0) > 1000 THEN CAST(CAST(((mf.size * 8 / 1024)) AS DECIMAL(18,2)) AS VARCHAR(20))
    ELSE CAST(CAST((mf.size * 8 / 1024.0) AS DECIMAL(18,2)) AS VARCHAR(20))
    END AS FileSize, 
    CAST((mf.size * 8 / 1024.0) - (su.SpaceUsed / 128.0) AS DECIMAL(15,2)) FreeSpace, 
    mf.physical_name--,
	--RIGHT(mf.physical_name,4) as FileExtension
FROM     
	sys.databases d
	INNER JOIN sys.master_files mf 
	ON d.database_id = mf.database_id
	INNER JOIN #FixedDrives fd  
	ON LEFT(mf.physical_name,1) = fd.Drive
	INNER JOIN #SpaceUsed su    
	ON d.name = su.DbName AND mf.name = su.FileName
WHERE
	d.name NOT IN ('master', 'model', 'msdb', 'tempdb')
	AND
	drive = @Drive
	AND
	type_desc = @FileType
	AND 
	--mf.is_read_only = 0
	d.is_read_only = 0 -- filter out read-only databases
	AND
	d.state = 0 -- filter out non-online databases
)
SELECT
	drive,
	DiskFreeSpace,
	DatabaseId,
	DatabaseName,
	FileType,
	[Filename],
	physical_name,
	FileSize,
	FreeSpace,
	FORMAT(FreeSpace / FileSize,'P') AS [%FreeSpace],
	'USE '+DatabaseName+
	' DBCC SHRINKFILE (N'''+Filename+''''+','+CAST(CAST(FileSize-FreeSpace+1 AS INT)AS VARCHAR)+')' AS RemediationCommand
FROM
	AllResults
WHERE 
	CAST((FreeSpace / FileSize) * 100.00 AS INT) >= @MinimumPercentOfFreeSpace
ORDER BY
	[%FreeSpace] DESC
OPTION(RECOMPILE)
 
--Cleanup temp tables
DROP TABLE #FixedDrives 
DROP TABLE #SpaceUsed 