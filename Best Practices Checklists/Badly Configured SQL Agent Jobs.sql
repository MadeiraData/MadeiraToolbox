/*
=================================================================================
Description: This script finds SQL Agent jobs that are suspiciously misconfigured
Author: Eitan Blumin | https://www.madeiradata.com
Date: 2022-04-28
Last Update: 2022-04-28
=================================================================================
*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
  server_name = CONVERT(sysname, SERVERPROPERTY('ServerName'))
, job_name = j.name
, finding = issues.Issue
, j.enabled
, j.date_created
, j.date_modified
, job_owner = SUSER_SNAME(j.owner_sid)
, j.description
, category_name = cat.name
, counts.targetservers_count
, counts.steps_count
, counts.schedules_count
, counts.alerts_count
, counts.recent_runs_count
, remediation_example = issues.RemediationCmd
FROM msdb.dbo.sysjobs AS j
LEFT JOIN msdb.dbo.syscategories AS cat ON j.category_id = cat.category_id
CROSS APPLY
(
SELECT
  targetservers_count = (SELECT COUNT(*) FROM msdb.dbo.sysjobservers AS jsrv WHERE jsrv.job_id = j.job_id)
, steps_count = (SELECT COUNT(*) FROM msdb.dbo.sysjobsteps AS jstep WHERE jstep.job_id = j.job_id)
, schedules_count = (SELECT COUNT(*) FROM msdb.dbo.sysjobschedules AS jschd INNER JOIN msdb.dbo.sysschedules AS schd ON schd.schedule_id = jschd.schedule_id WHERE jschd.job_id = j.job_id AND schd.enabled = 1)
, alerts_count = (SELECT COUNT(*) FROM msdb.dbo.sysalerts AS alrt WHERE alrt.job_id = j.job_id AND alrt.enabled = 1)
, recent_runs_count = (SELECT COUNT(*) FROM msdb.dbo.sysjobhistory AS hist WHERE hist.job_id = j.job_id AND hist.run_date > CONVERT(varchar(1000), DATEADD(MONTH, -1, GETDATE()), 112))
) AS counts
CROSS APPLY
(
	SELECT Issue = N'No target server'
	, RemediationCmd = N'EXEC msdb.dbo.sp_add_jobserver @job_name=N' + QUOTENAME(j.name, N'''') + ', @server_name = N''(local)'';'
	WHERE counts.targetservers_count = 0
	UNION ALL
	SELECT Issue = N'No steps'
	, RemediationCmd = N'EXEC msdb.dbo.sp_add_jobstep @job_name=N' + QUOTENAME(j.name, N'''') + ', @step_name = N''step name'', @command = N''-- replace me with something'';'
	WHERE counts.steps_count = 0
	UNION ALL
	SELECT Issue = N'No enabled schedules or alerts, and no recent runs'
	, RemediationCmd = N'EXEC msdb.dbo.sp_add_jobschedule @job_name=N' + QUOTENAME(j.name, N'''') + ', @name=N''schedule name'', @enabled=1, @freq_type=4, @freq_interval=1, @freq_subday_type=1, @freq_subday_interval=0, @freq_relative_interval=0, @freq_recurrence_factor=1, @active_start_date=20220101, @active_end_date=99991231, @active_start_time=0, @active_end_time=235959;'
	WHERE counts.schedules_count = 0
	AND counts.recent_runs_count = 0 AND counts.alerts_count = 0
	AND cat.name NOT IN ('SQL Sentry Jobs', 'SentryOne Jobs')
	UNION ALL
	SELECT Issue = N'No valid owner'
	, RemediationCmd = N'EXEC msdb.dbo.sp_update_job @job_name=N' + QUOTENAME(j.name, N'''') + ', @owner_login_name = N' + SUSER_SNAME(0x01) + N';'
	WHERE SUSER_SNAME(j.owner_sid) IS NULL
) AS issues
WHERE j.date_created < DATEADD(MINUTE, -15, GETDATE()) -- allow grace period for newly created jobs
ORDER BY j.name