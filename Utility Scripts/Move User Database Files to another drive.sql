DECLARE @SourceDrive nvarchar(MAX) = 'C:\'
DECLARE @DataDestinationDrive nvarchar(MAX) = 'F'
DECLARE @LogDestinationDrive nvarchar(MAX) = 'E'

SELECT
N'ALTER DATABASE ' + QUOTENAME(DB_NAME(mf.database_id)) + N' MODIFY FILE ( NAME = ' + QUOTENAME(mf.name)
+ N', FILENAME = ' + QUOTENAME(STUFF(mf.physical_name, 1, LEN(v.newdrive), v.newdrive), N'''') +  N' );'
FROM sys.master_files AS mf
CROSS APPLY
(VALUES
(CASE WHEN mf.type_desc = 'ROWS' THEN @DataDestinationDrive ELSE @LogDestinationDrive END)
) AS v(newdrive)
WHERE mf.physical_name LIKE @SourceDrive + N'%'
AND mf.type IN (0,1)
