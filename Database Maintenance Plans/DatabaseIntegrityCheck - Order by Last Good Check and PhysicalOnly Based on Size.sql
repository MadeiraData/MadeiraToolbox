/*
DatabaseIntegrityCheck - Normal checks on smaller databases, PhysicalOnly on large databases
==========================================================================
Author: Eitan Blumin
Date: 2023-06-19
Description:

Prerequisites:
	- Ola Hallengren's maintenance solution installed. This script must run within the context of the database where it was installed.
	- Ola Hallengren's maintenance solution can be downloaded for free from here: https://ola.hallengren.com
	- SQL Server version 2012 or newer.
*/
DECLARE @TimeLimitMinutes			int	= 60 * 24
DECLARE @PhysicalOnlyMBThreshold	int	= 1024



DECLARE @SmallDatabasesList nvarchar(MAX) = 'ALL_DATABASES', @LargeDatabasesList nvarchar(MAX), @TimeLimitSeconds int;
SET @TimeLimitSeconds = @TimeLimitMinutes * 60

SELECT
  @SmallDatabasesList = @SmallDatabasesList + N',-' + DB_NAME(database_id)
, @LargeDatabasesList = ISNULL(@LargeDatabasesList + N',',N'') + DB_NAME(database_id)
FROM sys.master_files
WHERE type = 0
AND DB_NAME(database_id) NOT IN ('tempdb','model')
GROUP BY database_id
HAVING SUM(size) / 128 >= @PhysicalOnlyMBThreshold

-- Small databases
EXEC dbo.DatabaseIntegrityCheck
	@Databases = @SmallDatabasesList,
	@TimeLimit = @TimeLimitSeconds,
	@LogToTable= 'Y',
	@Execute = 'Y'

-- Large databases
IF @LargeDatabasesList IS NOT NULL
BEGIN
	EXEC dbo.DatabaseIntegrityCheck
		@Databases = @LargeDatabasesList,
		@TimeLimit = @TimeLimitSeconds,
		@PhysicalOnly = 'Y',
		@LogToTable= 'Y',
		@Execute = 'Y'
END