USE DBA
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create function [dbo].[GetLocksChainFunction]()
returns table
as 
return 
(

with RecLocks
as
(
select  cast(cast(not_blocked.spid as varchar(10)) as varchar(4000)) as chain,
not_blocked.blocked,not_blocked.spid
from sys.sysprocesses not_blocked with(nolock)
	join sys.sysprocesses blocked with(nolock)
		on not_blocked.spid = blocked.blocked
where  not_blocked.blocked = 0

union all

select cast(blocking.chain + '->'+
	  cast(blocked.spid as varchar(10)) as varchar(4000))as chain, 
		blocked.blocked, blocked.spid
from RecLocks blocking
	join sys.sysprocesses blocked with(nolock)
		on blocking.spid = blocked.blocked
		and blocking.spid <> blocked.spid
)
select r1.chain as LockChain
from RecLocks r1
	left join RecLocks r2
		on charindex(r1.chain, r2.chain, 0) > 0
		and r1.chain <> r2.chain
where r1.blocked <> 0
and r2.chain is null
)
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[Locks](
	[spid] [smallint] NOT NULL,
	[kpid] [smallint] NOT NULL,
	[blocked] [smallint] NOT NULL,
	[waittype] [binary](2) NOT NULL,
	[waittime] [bigint] NOT NULL,
	[lastwaittype] [nchar](32) NOT NULL,
	[waitresource] [nchar](256) NOT NULL,
	[dbid] [smallint] NOT NULL,
	[uid] [smallint] NULL,
	[cpu] [int] NOT NULL,
	[physical_io] [bigint] NOT NULL,
	[memusage] [int] NOT NULL,
	[login_time] [datetime] NOT NULL,
	[last_batch] [datetime] NOT NULL,
	[ecid] [smallint] NOT NULL,
	[open_tran] [smallint] NOT NULL,
	[status] [nchar](30) NOT NULL,
	[sid] [binary](86) NOT NULL,
	[hostname] [nchar](128) NOT NULL,
	[program_name] [nchar](128) NOT NULL,
	[hostprocess] [nchar](10) NOT NULL,
	[cmd] [nchar](16) NOT NULL,
	[nt_domain] [nchar](128) NOT NULL,
	[nt_username] [nchar](128) NOT NULL,
	[net_address] [nchar](12) NOT NULL,
	[net_library] [nchar](12) NOT NULL,
	[loginame] [nchar](128) NOT NULL,
	[context_info] [binary](128) NOT NULL,
	[sql_handle] [binary](20) NOT NULL,
	[stmt_start] [int] NOT NULL,
	[stmt_end] [int] NOT NULL,
	[request_id] [int] NOT NULL,
	[blocking_program_name] [nchar](128) NOT NULL,
	[blocked_text] [nvarchar](max) NULL,
	[blocking_text] [nvarchar](max) NULL,
	[lock_date] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

CREATE CLUSTERED INDEX IX_TimeStamp ON [dbo].[Locks] ([lock_date]);
GO
SET ANSI_PADDING OFF
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[LocksChain](
	[TimeStamp] [datetime] NULL DEFAULT (getdate()),
	[LocksChain] [varchar](4000) NULL
) ON [PRIMARY]

CREATE CLUSTERED INDEX IX_TimeStamp ON [dbo].[LocksChain] ([TimeStamp]);
GO
SET ANSI_PADDING OFF
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC CollectLocks
	  @timeOfBlockInSec int = 5
	, @sendMail BIT = 0
	, @mailRecipient varchar(4000) = 'sqlalerts@companydomain.com'
	, @mailFrequencyInMinutes int = 10
	, @mailProfile sysname = null
AS
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF EXISTS (SELECT * FROM sys.dm_exec_requests
CROSS APPLY sys.dm_exec_sql_text(sql_handle)
WHERE text LIKE 'CREATE PROC CollectLocks%'
AND session_id <> @@SPID)
BEGIN
	PRINT 'CollectLocks already running. Aborting...'
	RETURN;
END

DECLARE @date DATETIME, @lockDate DATETIME

WHILE 1=1
BEGIN
IF (SELECT COUNT(*)
FROM sys.sysprocesses blocked
	JOIN  sys.sysprocesses blocking
		ON blocked.blocked = blocking.spid
WHERE blocked.blocked <> 0
	AND blocked.waittime/1000 > ISNULL(@timeOfBlockInSec,5)) > 0
BEGIN
	
	SET @lockDate = GETDATE();

	INSERT INTO dbo.LocksChain 
	select @lockDate,* from dbo.GetLocksChainFunction()
	OPTION (MAXRECURSION 0)
	
	INSERT INTO dbo.Locks
	SELECT DISTINCT blocked.*,blocking.program_name AS [blocking_program_name],blocked_text.text AS blocked_text, blocking_text.text AS [blocking_text],@lockDate
	FROM sys.sysprocesses blocked
			JOIN  sys.sysprocesses blocking
				ON blocked.blocked = blocking.spid
		OUTER APPLY sys.dm_exec_sql_text(blocked.sql_handle) blocked_text
		OUTER APPLY sys.dm_exec_sql_text(blocking.sql_handle) blocking_text
		WHERE blocked.blocked <> 0
			AND blocked.waittime/1000 > ISNULL(@timeOfBlockInSec,5)	
	
	IF @@ROWCOUNT > 0 AND @sendMail = 1 AND (@date IS NULL OR DATEDIFF(MINUTE, @date, GETDATE()) > @mailFrequencyInMinutes)
	BEGIN
		SET @date  = GETDATE()

		DECLARE @html NVARCHAR(MAX);
			SET @html = '<style>td ,th {text-align: left; border:1px solid black;} table {width: 60%; border:1px solid black; border-collapse:collapse;} </style>
			<h1>Locks Found</h1><h2>'

		SELECT @html += '<table border=1><tr><th>Blocked Spid</th><th>Blocked Program</th><th>Blocked Waittime(Sec)</th><th>Blocked Resource</th><th>Blocking Spid</th><th>Blocking Program</th><th>Blocked Text</th><th>Blocking Text</th></tr>';
			
		SELECT  @html += '<tr><td>'
							+ CAST(spid AS VARCHAR(20)) + '</td><td>'
							+ isnull(program_name,'') + '</td><td>'
							+ cast(waittime/1000 AS VARCHAR(20))+ '</td><td>'
							+ waitresource + '</td><td>'
							+ cast(blocked AS VARCHAR(20)) + '</td><td>'						
							+ isnull(blocking_program_name,'') + '</td><td>'
							+ isnull(substring(blocked_text,0,100),'') + '</td><td>'												
							+ isnull(substring(blocking_text,0,100),'') + '</td></tr>'		 
		FROM dbo.Locks
		WHERE lock_date = @lockDate

		SELECT @html += '</table>'	
		
		SELECT @html += '<table><tr><th>Locks Chain</th></tr>'
		SELECT  DISTINCT @html += '<tr><td>'
			+ LocksChain + '</td></tr>'
		FROM dbo.LocksChain
		WHERE [TimeStamp] = @lockDate
		
		SELECT @html += '</table>'

		EXEC msdb.dbo.sp_send_dbmail @profile_name=@mailProfile, @recipients = @mailRecipient, 
					@subject = 'Locks Found', @body = @html, @importance = 'High', @body_format ='HTML';	
	END	
END
WAITFOR DELAY '00:00:05'
END
GO