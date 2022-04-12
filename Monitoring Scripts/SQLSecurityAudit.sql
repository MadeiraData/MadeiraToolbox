USE [master]
GO
SET NOCOUNT ON;
DECLARE   @PreviousDateThreshold datetime
	, @ExtendedPropertyName sysname --= N'Madeira_SQLSecurityAudit_LastSampleTime'

IF @ExtendedPropertyName IS NOT NULL
BEGIN
	-- Retrieve the last datetime threshold from previous sample
	SELECT @PreviousDateThreshold = CONVERT(datetime, [value])
	FROM sys.extended_properties
	WHERE [name] = @ExtendedPropertyName
END

DECLARE @LogDirectory nvarchar(4000), @cmd nvarchar(MAX);

IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 11
BEGIN
	SELECT @LogDirectory = [path]
	FROM sys.dm_os_server_diagnostics_log_configurations
	OPTION(RECOMPILE)
END
ELSE
BEGIN
	SELECT @LogDirectory = LEFT(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max)),LEN(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max))) - CHARINDEX('\',REVERSE(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max)))))
END

IF NOT EXISTS (SELECT * FROM sys.server_audits WHERE name = 'SQLSecurityAudit')
BEGIN
	RAISERROR(N'Creating server audit SQLSecurityAudit',0,1) WITH NOWAIT;
	SET @cmd = N'
	CREATE SERVER AUDIT [SQLSecurityAudit]
	TO FILE 
	(	FILEPATH = ' + QUOTENAME(@LogDirectory, N'''') + N'
		,MAXSIZE = 100 MB
		,MAX_ROLLOVER_FILES = 3
		,RESERVE_DISK_SPACE = OFF
	)
	WITH
	(	QUEUE_DELAY = 1000
		,ON_FAILURE = CONTINUE
	);'
	EXEC(@cmd);
END
ELSE
	RAISERROR(N'Server audit SQLSecurityAudit already exists',0,1) WITH NOWAIT;

IF EXISTS (SELECT * FROM sys.server_audits WHERE name = 'SQLSecurityAudit' AND is_state_enabled = 0)
BEGIN
	RAISERROR(N'Enabling server audit SQLSecurityAudit',0,1) WITH NOWAIT;

	ALTER SERVER AUDIT [SQLSecurityAudit] WITH (STATE = ON);  
END
ELSE
	RAISERROR(N'Server audit SQLSecurityAudit already enabled',0,1) WITH NOWAIT;

IF NOT EXISTS (SELECT * FROM sys.server_audit_specifications WHERE name = 'SQLSecurityAuditSpecification')
BEGIN
	RAISERROR(N'Creating server audit specification SQLSecurityAuditSpecification',0,1) WITH NOWAIT;
	
	CREATE SERVER AUDIT SPECIFICATION [SQLSecurityAuditSpecification] FOR SERVER AUDIT [SQLSecurityAudit];
END
ELSE
	RAISERROR(N'Server audit specification SQLSecurityAuditSpecification already exists',0,1) WITH NOWAIT;

-- Check for missing audited actions
SET @cmd = NULL;
SELECT @cmd = ISNULL(@cmd + N',' + CHAR(13), N'') + N' ADD (' + q.covering_parent_action_name + N')'
FROM (SELECT DISTINCT covering_parent_action_name
FROM sys.dm_audit_actions
WHERE parent_class_desc = 'SERVER'
AND covering_parent_action_name LIKE '%CHANGE%'
AND covering_parent_action_name NOT IN
('DATABASE_CHANGE_GROUP','DATABASE_OBJECT_CHANGE_GROUP','SCHEMA_OBJECT_CHANGE_GROUP','SERVER_OBJECT_CHANGE_GROUP')
) AS q
WHERE NOT EXISTS
(
SELECT TOP (1) NULL
FROM sys.server_audit_specification_details  AS sasd
INNER JOIN sys.server_audit_specifications AS sas ON sasd.server_specification_id = sas.server_specification_id
INNER JOIN sys.server_file_audits AS sfa ON sas.audit_guid = sfa.audit_guid
WHERE sas.name = 'SQLSecurityAuditSpecification'
AND q.covering_parent_action_name COLLATE DATABASE_DEFAULT = sasd.audit_action_name COLLATE DATABASE_DEFAULT
)

IF @@ROWCOUNT > 0
BEGIN
	IF EXISTS (SELECT * FROM sys.server_audit_specifications WHERE is_state_enabled = 1 AND name = 'SQLSecurityAuditSpecification')
	BEGIN
		RAISERROR(N'Disabling server audit specification SQLSecurityAuditSpecification',0,1) WITH NOWAIT;

		ALTER SERVER AUDIT SPECIFICATION SQLSecurityAuditSpecification WITH (STATE=OFF);  
	END

	SET @cmd = N'ALTER SERVER AUDIT SPECIFICATION [SQLSecurityAuditSpecification] FOR SERVER AUDIT [SQLSecurityAudit]'
	+ @cmd

	PRINT @cmd
	EXEC (@cmd)
	
	RAISERROR(N'Enabling server audit specification SQLSecurityAuditSpecification',0,1) WITH NOWAIT;

	ALTER SERVER AUDIT SPECIFICATION SQLSecurityAuditSpecification WITH (STATE=ON);  
END
ELSE
	RAISERROR(N'Server audit specification SQLSecurityAuditSpecification already fully configured',0,1) WITH NOWAIT;

DECLARE @AuditName sysname, @File nvarchar(4000);

SELECT TOP (1) @AuditName = sas.name, @File = log_file_path + REPLACE(log_file_name, '.sqlaudit', '*.sqlaudit')
FROM sys.server_audit_specification_details  AS sasd
INNER JOIN sys.server_audit_specifications AS sas ON sasd.server_specification_id = sas.server_specification_id
INNER JOIN sys.server_file_audits AS sfa ON sas.audit_guid = sfa.audit_guid
WHERE EXISTS
(
SELECT NULL
FROM sys.dm_audit_actions AS daa
WHERE parent_class_desc = 'SERVER'
AND covering_parent_action_name LIKE '%CHANGE%'
AND daa.covering_parent_action_name COLLATE DATABASE_DEFAULT = sasd.audit_action_name COLLATE DATABASE_DEFAULT
)

RAISERROR(N'Audit: "%s", File: "%s"',0,1,@AuditName,@File) WITH NOWAIT;

SELECT event_time, session_server_principal_name, host_name, client_ip, application_name, statement
FROM sys.fn_get_audit_file(@File, default, default)
WHERE statement <> N''
AND (@PreviousDateThreshold IS NULL OR event_time > @PreviousDateThreshold)
ORDER BY event_time DESC
OPTION (RECOMPILE);

IF @@ROWCOUNT > 0 AND @ExtendedPropertyName IS NOT NULL
BEGIN
	DECLARE @NewDateThreshold datetime;

	SELECT @NewDateThreshold = MAX(event_time)
	FROM sys.fn_get_audit_file(@File, default, default)
	WHERE statement <> N''
	AND (@PreviousDateThreshold IS NULL OR event_time > @PreviousDateThreshold)
	OPTION (RECOMPILE);

	IF @NewDateThreshold IS NOT NULL
	BEGIN
		-- Save the last sampled event time:
		IF NOT EXISTS
		(
			SELECT *
			FROM sys.extended_properties
			WHERE [name] = @ExtendedPropertyName
		)
		BEGIN
			EXEC sp_addextendedproperty @name = @ExtendedPropertyName, @value = @NewDateThreshold;  
		END
		ELSE
		BEGIN
			EXEC sp_updateextendedproperty @name = @ExtendedPropertyName, @value = @NewDateThreshold;
		END
	END
END