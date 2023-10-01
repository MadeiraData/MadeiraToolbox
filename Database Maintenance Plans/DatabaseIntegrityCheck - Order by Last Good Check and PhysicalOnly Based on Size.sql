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
DECLARE @MaxEndTime				datetime = DATEADD(HOUR, 24, GETDATE())
DECLARE @PhysicalOnlyMBThreshold	int	 = 1024


DECLARE @SmallDatabasesList nvarchar(MAX) = 'ALL_DATABASES', @LargeDatabasesList nvarchar(MAX), @TimeLimitSeconds int;

SELECT
  @SmallDatabasesList = @SmallDatabasesList + N',-' + DB_NAME(database_id)
, @LargeDatabasesList = ISNULL(@LargeDatabasesList + N',',N'') + DB_NAME(database_id)
FROM sys.master_files
WHERE type = 0
AND DB_NAME(database_id) NOT IN ('tempdb','model')
GROUP BY database_id
HAVING SUM(size) / 128 >= @PhysicalOnlyMBThreshold

-- Small databases
SET @TimeLimitSeconds = DATEDIFF(second, GETDATE(), @MaxEndTime)

EXEC dbo.DatabaseIntegrityCheck
	@Databases = @SmallDatabasesList,
	@DatabaseOrder = 'DATABASE_LAST_GOOD_CHECK_ASC',
	@TimeLimit = @TimeLimitSeconds,
	@LogToTable= 'Y',
	@Execute = 'Y'

-- Large databases
SET @TimeLimitSeconds = DATEDIFF(second, GETDATE(), @MaxEndTime)

IF @LargeDatabasesList IS NOT NULL AND @TimeLimitSeconds > 0
BEGIN
	EXEC dbo.DatabaseIntegrityCheck
		@Databases = @LargeDatabasesList,
		@DatabaseOrder = 'DATABASE_LAST_GOOD_CHECK_ASC',
		@TimeLimit = @TimeLimitSeconds,
		@PhysicalOnly = 'Y',
		@LogToTable= 'Y',
		@Execute = 'Y'
END