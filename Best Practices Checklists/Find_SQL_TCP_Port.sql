USE master
GO
-- Using SQL Error Logs:

xp_readerrorlog 0, 1, N'Server is listening on', N'any', NULL, NULL, N'asc' 
-- will also return records for DB Mirroring endpoints
-- also, this won't work if error log was cycled
GO

-- Using currently connected connections:

SELECT distinct local_tcp_port
FROM   sys.dm_exec_connections
WHERE  local_tcp_port is not null
-- will also return records for DB Mirroring endpoints

GO

-- Using system registry (dynamic port):

DECLARE       @portNo   NVARCHAR(10)
  
EXEC   xp_instance_regread
@rootkey    = 'HKEY_LOCAL_MACHINE',
@key        =
'Software\Microsoft\Microsoft SQL Server\MSSQLServer\SuperSocketNetLib\Tcp\IpAll',
@value_name = 'TcpDynamicPorts',
@value      = @portNo OUTPUT
  
SELECT [PortNumber] = @portNo

GO

-- Using system registry (static port):

DECLARE       @portNo   NVARCHAR(10)
  
EXEC   xp_instance_regread
@rootkey    = 'HKEY_LOCAL_MACHINE',
@key        =
'Software\Microsoft\Microsoft SQL Server\MSSQLServer\SuperSocketNetLib\Tcp\IpAll',
@value_name = 'TcpPort',
@value      = @portNo OUTPUT
  
SELECT [PortNumber] = @portNo
GO
