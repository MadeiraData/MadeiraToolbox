
-- Generate disable / enable commands at the database level:
SELECT DatabaseName = DB_NAME()
, DisableCmd = CONCAT(N'ALTER DATABASE ', QUOTENAME(DB_NAME()), N' SET CHANGE_TRACKING = OFF;')
, EnableCmd = CONCAT(N'ALTER DATABASE ', QUOTENAME(DB_NAME()), N' SET CHANGE_TRACKING = ON (CHANGE_RETENTION = ', retention_period, N' ', retention_period_units_desc, N', AUTO_CLEANUP = ', CASE is_auto_cleanup_on WHEN 1 THEN N'ON' ELSE N'OFF' END, N');')
, *
FROM sys.change_tracking_databases
WHERE database_id = DB_ID()

-- Generate disable / enable commands at the table level:
select TableName = object_name(object_id)
, DisableCmd = CONCAT(N'USE ', QUOTENAME(DB_NAME()), N'; ALTER TABLE ',object_name(object_id), ' DISABLE CHANGE_TRACKING;')
, EnableCmd = CONCAT(N'USE ', QUOTENAME(DB_NAME()), N'; ALTER TABLE ',object_name(object_id), ' ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ', CASE is_track_columns_updated_on WHEN 1 THEN N'ON' ELSE N'OFF' END, N');')
, *
from sys.change_tracking_tables
