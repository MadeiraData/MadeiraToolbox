SELECT N'In server ' + @@SERVERNAME + N', owner of job "' + j.name COLLATE database_default + N'"'
+ CASE WHEN L.sid IS NULL THEN N' was not found'
	   WHEN L.denylogin = 1 OR L.hasaccess = 0 THEN N' (' + L.name COLLATE database_default + N') has no server access'
	   ELSE N' is ok'
END
, 1
, RemediationCmd = N'EXEC msdb.dbo.sp_update_job @job_name=N' + QUOTENAME(j.name,N'''') + N' , @owner_login_name=N' + QUOTENAME(SUSER_NAME(0x01), N'''')
FROM msdb.dbo.sysjobs j
LEFT JOIN master.sys.syslogins L on j.owner_sid = L.sid
WHERE j.enabled = 1
AND (L.sid IS NULL OR L.denylogin = 1 OR L.hasaccess = 0)
