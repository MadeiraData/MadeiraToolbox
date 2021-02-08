/*
Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
Description: Create a Linked Server to a remote SQL Server, but give it a different name than its actual address
*/
DECLARE
	@ServerAddress 		[nvarchar](255) = 'MyRemoteServerAddress\SomeNamedInstanceIfYouWant,1433',
	@NewServerName 		[nvarchar](255) = 'MyRemoteServerName',
	@RemoteUser 		[nvarchar](128) = 'remote_user', -- login name of remote mapped user. If NULL, will not create a mapped login.
	@RemotePassword		[nvarchar](128) = 'remote_user_password',
	@MapLocalLogin 		[nvarchar](255) = NULL -- name a local login to map to the remote login. If NULL, will map current login.

SET @MapLocalLogin = ISNULL(@MapLocalLogin, SUSER_NAME())

DECLARE @ProviderString NVARCHAR(100);
SET @ProviderString = N'PROVIDER=SQLNCLI;SERVER=tcp:' + @ServerAddress

-- If linked server already exists, drop it first
IF EXISTS (SELECT srv.name FROM sys.servers srv WHERE srv.server_id != 0 AND srv.name = @NewServerName)
	EXEC master.dbo.sp_dropserver @server=@NewServerName, @droplogins='droplogins'

EXEC master.dbo.sp_addlinkedserver @server = @NewServerName, @provider = N'SQLNCLI', @srvproduct=N'MSSQL', @provstr=@ProviderString;

EXEC master.dbo.sp_serveroption @server=@NewServerName, @optname=N'collation compatible', @optvalue=N'true'
EXEC master.dbo.sp_serveroption @server=@NewServerName, @optname=N'data access', @optvalue=N'true'
EXEC master.dbo.sp_serveroption @server=@NewServerName, @optname=N'rpc', @optvalue=N'true'
EXEC master.dbo.sp_serveroption @server=@NewServerName, @optname=N'rpc out', @optvalue=N'true'
EXEC master.dbo.sp_serveroption @server=@NewServerName, @optname=N'connect timeout', @optvalue=N'0'
EXEC master.dbo.sp_serveroption @server=@NewServerName, @optname=N'collation name', @optvalue=null
EXEC master.dbo.sp_serveroption @server=@NewServerName, @optname=N'query timeout', @optvalue=N'0'
EXEC master.dbo.sp_serveroption @server=@NewServerName, @optname=N'use remote collation', @optvalue=N'true'
EXEC master.dbo.sp_serveroption @server=@NewServerName, @optname=N'remote proc transaction promotion', @optvalue=N'false'

IF @RemoteUser IS NOT NULL
	EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname = @NewServerName, @locallogin = @MapLocalLogin , @useself = N'False', @rmtuser = @RemoteUser, @rmtpassword = @RemotePassword
