/*
Orphaned SQL Agent Jobs
=======================
Author: Eitan Blumin
Date: 2021-04-25
Description:
This script detects any SQL Agent jobs whose owners do not exist as valid server logins.
The script also checks whether said owners are members of domain groups with sysadmin permissions.
The output would contain full details, including a remediation command to change the job owner to "sa".

IMPORTANT NOTE:
Only members of the sysadmin server role are allowed to modify jobs not owned by themselves.
Therefore, it's important to make sure that any existing logins would retain their ability
to maintain their own jobs. If they already have sysadmin permissions, then it'll be okay.
But if they don't, then they would lose access to their previously-owned job(s).

Therefore, remember to check whether it's okay to change a job's owner to "sa", based on whether:
1. The current owner no longer requires access to modify the job (unused login, no longer employee, etc.).
2. The current owner is a member of the sysadmin server role.
*/
SET NOCOUNT ON;
DECLARE @AllowOnlySysadminsAsJobOwners bit = 1

-- Find any Windows Domain Groups which were given sysadmin permissions:
DECLARE @AdminsByGroup AS TABLE (AccountName sysname, AccountType sysname, privilege sysname, MappedName sysname, GroupPath sysname);
DECLARE @CurrentGroup sysname;

DECLARE Groups CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.server_principals
WHERE IS_SRVROLEMEMBER('sysadmin', [name]) = 1
AND type = 'G'

OPEN Groups;

WHILE 1=1
BEGIN
	FETCH NEXT FROM Groups INTO @CurrentGroup;
	IF @@FETCH_STATUS <> 0 BREAK;

	BEGIN TRY
	INSERT INTO @AdminsByGroup
	EXEC master..xp_logininfo 
		@acctname = @CurrentGroup,
		@option = 'members';
	END TRY
	BEGIN CATCH
		PRINT ERROR_MESSAGE()
	END CATCH
END

CLOSE Groups;
DEALLOCATE Groups;

-- Find orphaned SQL Agent jobs and determine whether current owner has sysadmin permissions:
SELECT
  JobName = j.name
, CurrentOwner = SUSER_SNAME(j.owner_sid)
, IsSysAdmin = ISNULL(IS_SRVROLEMEMBER('sysadmin', SUSER_SNAME(j.owner_sid)), 0)
, IsMemberOfSysAdminGroup = CASE WHEN g.AccountName IS NOT NULL THEN 1 ELSE 0 END
, MemberOfSysAdminGroup = g.GroupPath
, Issue =
CASE WHEN SUSER_SNAME(j.owner_sid) IS NULL THEN N'Account/Login does not exist'
WHEN L.sid IS NULL THEN N'Not found in server logins'
WHEN L.denylogin = 1 OR L.hasaccess = 0 THEN N'Login has no server access'
WHEN (@AllowOnlySysadminsAsJobOwners = 1 AND IS_SRVROLEMEMBER('sysadmin', SUSER_SNAME(j.owner_sid)) = 0) THEN N'Owner is not sysadmin'
ELSE N'OK (wait, why are you seeing this?)'
END
, RemediationCmd = N'EXEC msdb.dbo.sp_update_job @job_name=N' + QUOTENAME(j.name,N'''') + N' , @owner_login_name=N' + QUOTENAME(SUSER_SNAME(0x01), N'''')
, hist.last_run_date_time
, hist.last_run_duration
, hist.last_outcome_status
, hist.last_outcome_message
, sch.next_run_date_time
FROM msdb.dbo.sysjobs j
LEFT JOIN master.sys.syslogins L on j.owner_sid = L.sid
OUTER APPLY
(
	SELECT TOP (1)
	  last_run_date_time = msdb.dbo.agent_datetime(jh.run_date, jh.run_time)
	, last_run_duration = msdb.dbo.agent_datetime(20000101, jh.run_duration) - '2000-01-01'
	, last_outcome_status = CASE jh.run_status
							WHEN 0 THEN N'Failed'
							WHEN 1 THEN N'Succeeded'
							WHEN 2 THEN N'Retry'
							WHEN 3 THEN N'Canceled'
							WHEN 4 THEN N'In Progress'
							ELSE N'Unknown'
						END
	, last_outcome_message = jh.message
	FROM msdb.dbo.sysjobhistory AS jh
	WHERE jh.job_id = j.job_id
	AND jh.run_date > 0
	ORDER BY jh.run_date DESC, jh.run_time DESC
) AS hist
OUTER APPLY
(
	SELECT TOP (1) msdb.dbo.agent_datetime(jsch.next_run_date, jsch.next_run_time) AS next_run_date_time
	FROM msdb.dbo.sysjobschedules AS jsch
	WHERE jsch.job_id = j.job_id
	AND jsch.next_run_date > 0
	ORDER BY jsch.next_run_date ASC, jsch.next_run_time ASC
) AS sch
OUTER APPLY
(
	SELECT TOP (1) AccountName, GroupPath
	FROM @AdminsByGroup
	WHERE AccountName = SUSER_SNAME(j.owner_sid)
) AS g
WHERE j.enabled = 1
AND (
	SUSER_SNAME(j.owner_sid) IS NULL OR L.sid IS NULL OR L.denylogin = 1 OR L.hasaccess = 0
	OR
	(@AllowOnlySysadminsAsJobOwners = 1 AND IS_SRVROLEMEMBER('sysadmin', SUSER_SNAME(j.owner_sid)) = 0)
)
