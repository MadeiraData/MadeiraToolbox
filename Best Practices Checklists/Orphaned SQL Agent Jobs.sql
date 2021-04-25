SELECT j.*
FROM msdb.dbo.sysjobs j
LEFT JOIN master.sys.syslogins L on j.owner_sid = L.sid
WHERE L.sid IS NULL AND j.enabled = 1