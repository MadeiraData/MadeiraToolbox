SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
	 @EnabledJobsOnly	bit = 0
	,@EnabledSchedulesOnly	bit = 0

SELECT m.[name], SUSER_SNAME([m].[ownersid]) AS [owner], m.createdate
-- UPDATE m SET m.ownersid = 0x01 -- set owner to sa. you could also use SUSER_SID('MyLoginName')
-- OUTPUT inserted.[name] AS maintenance_plan, SUSER_SNAME(deleted.ownersid) AS old_owner, SUSER_SNAME(inserted.ownersid) AS new_owner
FROM msdb.dbo.sysssispackages AS m
WHERE m.packagetype = 6
AND m.id IN (SELECT id FROM msdb.dbo.sysmaintplan_plans)
AND IS_SRVROLEMEMBER('sysadmin', SUSER_SNAME([m].[ownersid])) = 0
AND NOT EXISTS
(
	SELECT *
	FROM msdb.dbo.sysproxyloginsubsystem_view AS p
	INNER JOIN msdb.dbo.sysjobsteps AS js ON js.proxy_id = p.proxy_id
	INNER JOIN msdb.dbo.sysjobs AS j ON j.job_id = js.job_id
	INNER JOIN msdb.dbo.sysmaintplan_subplans AS sp ON sp.job_id = js.job_id
	LEFT JOIN msdb.dbo.sysschedules AS jsch ON sp.schedule_id = jsch.schedule_id
	WHERE subsystem_id = 11 -- SSIS
	AND sp.plan_id = m.id
	AND SUSER_SNAME([sid]) = m.[ownersid]
	AND (NULLIF(@EnabledJobsOnly,0) IS NULL OR j.enabled = 1)
	AND (NULLIF(@EnabledSchedulesOnly,0) IS NULL OR jsch.enabled = 1)
)
GO
