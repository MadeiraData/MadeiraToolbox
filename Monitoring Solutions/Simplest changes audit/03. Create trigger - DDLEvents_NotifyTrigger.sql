USE [YourDBname]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TRIGGER [YourSchemaName].[DDLEvents_NotifyTrigger] 
	ON  [YourDBname].[YourSchemaName].[DDLEventsAudit] 
	AFTER INSERT
AS 
BEGIN

	SET NOCOUNT ON;

	DECLARE
		@OperatorName				NVARCHAR(128) = (SELECT TOP (1) [Name] FROM msdb.dbo.sysmail_profile),
		@recipientsForAllEvents		NVARCHAR(256) = N'recipient1@yourmaildomain.com; recipient2@yourmaildomain.com',
		@recipientsForLoginEvents	NVARCHAR(256) = (SELECT TOP (1) email_address FROM msdb.dbo.sysoperators ORDER BY id ASC),
		@SendRecipients				NVARCHAR(256),
		@mailSubject				NVARCHAR(200),
		@mailBody					NVARCHAR(MAX) = N'',

		@EventDate					DATETIME,
		@EventType					NVARCHAR(64),
		@EventXML					XML,
		@DatabaseName				NVARCHAR(255),
		@ObjectName					NVARCHAR(255),
		@HostName					VARCHAR(64),
		@IPAddress					VARCHAR(32),
		@ProgramName				NVARCHAR(255),
		@LoginName					NVARCHAR(255)
	

    DECLARE SendCursor CURSOR FOR

		SELECT
			[Type],
			[Database],
			[Object],
			[FromHost],
			IPAddress,
			[Program],
			[ByLogin]
		FROM
			inserted
		WHERE
			[Type] NOT IN ('ALTER_INDEX','UPDATE_STATISTICS')	-- Filter out index&statistics maintenance 
			AND
			[ByLogin] NOT IN ('Login_1', 'Login_2')				-- Filter out logins that you don't want to receive alerts for them

    OPEN SendCursor
    FETCH NEXT FROM SendCursor
	INTO @EventType, @DatabaseName, @ObjectName, @HostName, @IPAddress, @ProgramName, @LoginName

    WHILE @@FETCH_STATUS = 0
    BEGIN

		IF @EventType IN ('CREATE_LOGIN', 'ALTER_LOGIN', 'DROP_LOGIN', 'CREATE_USER', 'ALTER_USER', 'DROP_USER', 'ADD_ROLE_MEMBER', 'DROP_ROLE_MEMBER')
		BEGIN
			-- send e-mail with Login change details
			SET @SendRecipients = @recipientsForLoginEvents
		END
		ELSE
		BEGIN
			-- send e-mail with Schema change details
			SET @recipients = @recipientsForAllEvents
		END
	
		SET @mailSubject = '*** ' + @DatabaseName + ' - ' + @EventType + ' Event at server '+ @@SERVERNAME +' ***' 
	  
		
			SET @mailBody = @mailBody +
	
				N'<table border="1" cellpadding="3" cellspacing="1" style="border-collapse: collapse;">' +  
				N'<tr><th style="background-color: #000069; color: white;" colspan="7">Event details</th></tr>' +
				N'<tr style="background-color: #0073e6; color: black;"><th>ServerName</th><th>DBName</th><th>EventType</th><th>ObjectName</th><th>IPAddress</th><th>ProgramName</th><th>LoginName</th></tr>' +  
				CAST ( 
						(
						SELECT
							CASE WHEN (ROW_NUMBER() OVER (ORDER BY (SELECT NULL) DESC))%2 = 0 THEN '#fafaf3' ELSE '#f2f2f2' END AS "@bgcolor", '',
							td = @HostName, '',
							td = @DatabaseName, '',
							td = @EventType, '',
							td = @ObjectName, '',
							td = @IPAddress, '',
							td = REPLACE(@ProgramName, 'Microsoft SQL Server Management Studio', 'SSMS'), '',
							td = @LoginName, ''
							FOR XML PATH('tr'), TYPE 
						) AS NVARCHAR(MAX) ) +
				N'</table>' + 
				N'<br />' +
				N'<br />'


		EXEC msdb.dbo.sp_send_dbmail
					@profile_name		= @OperatorName,
					@SendRecipients		= @SendRecipients, 
					@subject			= @mailSubject,
					@body				= @mailBody,
					@body_format		= 'HTML';


      FETCH NEXT FROM SendCursor
	  INTO @EventType, @DatabaseName, @ObjectName, @HostName, @IPAddress, @ProgramName, @LoginName

    END
    CLOSE SendCursor
    DEALLOCATE SendCursor
    	
END
GO

ALTER TABLE [YourSchemaName].[DDLEventsAudit] ENABLE TRIGGER [DDLEvents_NotifyTrigger]
GO

