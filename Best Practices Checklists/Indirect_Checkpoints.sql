/*
Generate remediation commands to enable indirect checkpoints
============================================================
Based on query by Aaron Bertrand:
https://sqlperformance.com/2020/05/system-configuration/0-to-60-switching-to-indirect-checkpoints
*/
SELECT [name], target_recovery_time_in_seconds, RemediationCmd = N'ALTER DATABASE ' + QUOTENAME([name]) + ' SET TARGET_RECOVERY_TIME = 60 SECONDS;' 
FROM sys.databases AS d WITH(NOLOCK)
WHERE database_id > 4 
AND [name] NOT IN ('rdsadmin')
AND target_recovery_time_in_seconds = 0
AND [state] = 0
AND is_read_only = 0
AND is_distributor = 0
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE';