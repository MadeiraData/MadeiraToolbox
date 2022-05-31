-- ======================================
-- Execute the following code on Server A
-- ======================================

USE master
GO


-- Create a database master key

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '*****';
GO


-- Create a new certificate

CREATE CERTIFICATE [AGCertificate_ServerA]
WITH SUBJECT = 'AGCertificate - Server A', EXPIRY_DATE = '2999-12-31';
GO


-- Backup the public key of the certificate to the filesystem

BACKUP CERTIFICATE [AGCertificate_ServerA]
TO FILE = 'F:\BACKUP\AGCertificate_ServerA.cert'
GO


-- Create an endpoint for the Availability Group

CREATE ENDPOINT [Hadr_endpoint]
STATE = STARTED
AS TCP
(
	LISTENER_PORT = 5022
)
FOR DATABASE_MIRRORING
(
	AUTHENTICATION = CERTIFICATE AGCertificate_ServerA,
	ROLE = ALL, 
	ENCRYPTION = REQUIRED ALGORITHM AES
)
GO


-- ======================================
-- Execute the following code on Server B
-- ======================================

USE master
GO


-- Create a database master key

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '*****';
GO


-- Create a new certificate

CREATE CERTIFICATE [AGCertificate_ServerB]
WITH SUBJECT = 'AGCertificate - Server B', EXPIRY_DATE = '2999-12-31';
GO


-- Backup the public key of the certificate to the filesystem

BACKUP CERTIFICATE [AGCertificate_ServerB]
TO FILE = 'F:\BACKUP\AGCertificate_ServerB.cert'
GO


-- Create an endpoint for the Availability Group

CREATE ENDPOINT [Hadr_endpoint]
STATE = STARTED
AS TCP
(
	LISTENER_PORT = 5022
)
FOR DATABASE_MIRRORING
(
	AUTHENTICATION = CERTIFICATE AGCertificate_ServerB,
	ROLE = ALL, 
	ENCRYPTION = REQUIRED ALGORITHM AES
)
GO


-- ======================================
-- Execute the following code on Server C
-- ======================================

USE master
GO


-- Create a database master key

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '*****';
GO


-- Create a new certificate

CREATE CERTIFICATE [AGCertificate_ServerC]
WITH SUBJECT = 'AGCertificate - Server C', EXPIRY_DATE = '2999-12-31';
GO


-- Backup the public key of the certificate to the filesystem

BACKUP CERTIFICATE [AGCertificate_ServerC]
TO FILE = 'F:\BACKUP\AGCertificate_ServerC.cert'
GO


-- Create an endpoint for the Availability Group

CREATE ENDPOINT [Hadr_endpoint]
STATE = STARTED
AS TCP
(
	LISTENER_PORT = 5022
)
FOR DATABASE_MIRRORING
(
	AUTHENTICATION = CERTIFICATE AGCertificate_ServerC,
	ROLE = ALL, 
	ENCRYPTION = REQUIRED ALGORITHM AES
)
GO


-- ======================================
-- Execute the following code on Server A
-- ======================================

-- Create login for Server B

CREATE LOGIN [Login_ServerB] WITH PASSWORD = '*****';
GO


-- Create user for the login

CREATE USER [Login_ServerB] FOR LOGIN [Login_ServerB];
GO


-- Import the public key portion of the certificate from Server B

CREATE CERTIFICATE [AGCertificate_ServerB]
AUTHORIZATION [Login_ServerB]
FROM FILE = 'F:\BACKUP\AGCertificate_ServerB.cert';
GO


-- Grant the CONNECT permission to the login

GRANT CONNECT ON ENDPOINT::Hadr_endpoint TO [Login_ServerB];
GO


-- Create login for Server C

CREATE LOGIN [Login_ServerC] WITH PASSWORD = '*****';
GO


-- Create user for the login

CREATE USER [Login_ServerC] FOR LOGIN [Login_ServerC];
GO


-- Import the public key portion of the certificate from Server C

CREATE CERTIFICATE [AGCertificate_ServerC]
AUTHORIZATION [Login_ServerC]
FROM FILE = 'F:\BACKUP\AGCertificate_ServerC.cert';
GO


-- Grant the CONNECT permission to the login

GRANT CONNECT ON ENDPOINT::Hadr_endpoint TO [Login_ServerC];
GO


-- ======================================
-- Execute the following code on Server B
-- ======================================

-- Create login for Server A

CREATE LOGIN [Login_ServerA] WITH PASSWORD = '*****';
GO


-- Create user for the login

CREATE USER [Login_ServerA] FOR LOGIN [Login_ServerA];
GO


-- Import the public key portion of the certificate from Server A

CREATE CERTIFICATE [AGCertificate_ServerA]
AUTHORIZATION [Login_ServerA]
FROM FILE = 'F:\BACKUP\AGCertificate_ServerA.cert';
GO


-- Grant the CONNECT permission to the login

GRANT CONNECT ON ENDPOINT::Hadr_endpoint TO [Login_ServerA];
GO


-- Create login for Server C

CREATE LOGIN [Login_ServerC] WITH PASSWORD = '*****';
GO


-- Create user for the login

CREATE USER [Login_ServerC] FOR LOGIN [Login_ServerC];
GO


-- Import the public key portion of the certificate from Server C

CREATE CERTIFICATE [AGCertificate_ServerC]
AUTHORIZATION [Login_ServerC]
FROM FILE = 'F:\BACKUP\AGCertificate_ServerC.cert';
GO


-- Grant the CONNECT permission to the login

GRANT CONNECT ON ENDPOINT::Hadr_endpoint TO [Login_ServerC];
GO


-- ======================================
-- Execute the following code on Server C
-- ======================================

-- Create login for Server A

CREATE LOGIN [Login_ServerA] WITH PASSWORD = '*****';
GO


-- Create user for the login

CREATE USER [Login_ServerA] FOR LOGIN [Login_ServerA];
GO


-- Import the public key portion of the certificate from Server A

CREATE CERTIFICATE [AGCertificate_ServerA]
AUTHORIZATION [Login_ServerA]
FROM FILE = 'F:\BACKUP\AGCertificate_ServerA.cert';
GO


-- Grant the CONNECT permission to the login

GRANT CONNECT ON ENDPOINT::Hadr_endpoint TO [Login_ServerA];
GO


-- Create login for Server B

CREATE LOGIN [Login_ServerB] WITH PASSWORD = '*****';
GO


-- Create user for the login

CREATE USER [Login_ServerB] FOR LOGIN [Login_ServerB];
GO


-- Import the public key portion of the certificate from uk1wv7639

CREATE CERTIFICATE [AGCertificate_ServerB]
AUTHORIZATION [Login_ServerB]
FROM FILE = 'F:\BACKUP\AGCertificate_ServerB.cert';
GO


-- Grant the CONNECT permission to the login

GRANT CONNECT ON ENDPOINT::Hadr_endpoint TO [Login_ServerB];
GO


-- ======================================
-- Execute the following code on Server A
-- ======================================

-- Change the recovery model to full and the AUTO_CLOSE to off on all databases

USE
	master;
GO


ALTER DATABASE
	DB1
SET
	RECOVERY FULL;
GO


ALTER DATABASE
	DB1
SET
	AUTO_CLOSE OFF;
GO


ALTER DATABASE
	DB2
SET
	RECOVERY FULL;
GO


ALTER DATABASE
	DB2
SET
	AUTO_CLOSE OFF;
GO


ALTER DATABASE
	DB3
SET
	RECOVERY FULL;
GO


ALTER DATABASE
	DB3
SET
	AUTO_CLOSE OFF;
GO


ALTER DATABASE
	DB4
SET
	RECOVERY FULL;
GO


ALTER DATABASE
	DB4
SET
	AUTO_CLOSE OFF;
GO


ALTER DATABASE
	DB5
SET
	RECOVERY FULL;
GO


ALTER DATABASE
	DB5
SET
	AUTO_CLOSE OFF;
GO


-- Make a full backup of all the databases

BACKUP DATABASE DB1 TO DISK = 'F:\BACKUP\DB1.bak' WITH COMPRESSION;
GO

BACKUP DATABASE DB2 TO DISK = 'F:\BACKUP\DB2.bak' WITH COMPRESSION;
GO

BACKUP DATABASE DB3 TO DISK = 'F:\BACKUP\DB3.bak' WITH COMPRESSION;
GO

BACKUP DATABASE DB4 TO DISK = 'F:\BACKUP\DB4.bak' WITH COMPRESSION;
GO

BACKUP DATABASE DB5 TO DISK = 'F:\BACKUP\DB5.bak' WITH COMPRESSION;
GO


-- Create a new Availability Group with 3 replicas

CREATE AVAILABILITY GROUP [AG]
WITH
(
	AUTOMATED_BACKUP_PREFERENCE = PRIMARY,
	DB_FAILOVER = ON,
	DTC_SUPPORT = NONE
)
FOR DATABASE DB1 , DB2 , DB3 , DB4 , DB5
REPLICA ON
'ServerA' WITH
(
	ENDPOINT_URL = 'TCP://ServerA.DNSSuffix:5022', 
	FAILOVER_MODE = AUTOMATIC , 
	AVAILABILITY_MODE = SYNCHRONOUS_COMMIT , 
	SECONDARY_ROLE
	(
		ALLOW_CONNECTIONS = NO
	)
),
'ServerB' WITH
(
	ENDPOINT_URL = 'TCP://ServerB.DNSSuffix:5022', 
	FAILOVER_MODE = AUTOMATIC, 
	AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, 
	SECONDARY_ROLE
	(
		ALLOW_CONNECTIONS = NO
	)
),
'ServerC' WITH
(
	ENDPOINT_URL = 'TCP://ServerC.DNSSuffix:5022', 
	FAILOVER_MODE = MANUAL, 
	AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT, 
	SECONDARY_ROLE
	(
		ALLOW_CONNECTIONS = ALL
	)
);
GO


ALTER AVAILABILITY GROUP
	AG
ADD LISTENER 'AGListener'
(WITH IP ((N'XXX.XXX.XXX.XXX', N'255.255.255.0') , (N'XXX.XXX.XXX.XXX', N'255.255.255.0')) , PORT = 1433);
GO


-- ======================================
-- Execute the following code on Server B
-- ======================================

-- Join the Availability Group

ALTER AVAILABILITY GROUP AG JOIN;
GO


-- ======================================
-- Execute the following code on Server C
-- ======================================

-- Join the Availability Group

ALTER AVAILABILITY GROUP AG JOIN;
GO


-- ======================================
-- Execute the following code on Server A
-- ======================================

-- Make a log backup of all the databases

BACKUP LOG DB1 TO DISK = 'F:\BACKUP\DB1.trn' WITH COMPRESSION;
GO

BACKUP LOG DB2 TO DISK = 'F:\BACKUP\DB2.trn' WITH COMPRESSION;
GO

BACKUP LOG DB3 TO DISK = 'F:\BACKUP\DB3.trn' WITH COMPRESSION;
GO

BACKUP LOG DB4 TO DISK = 'F:\BACKUP\DB4.trn' WITH COMPRESSION;
GO

BACKUP LOG DB5 TO DISK = 'F:\BACKUP\DB5.trn' WITH COMPRESSION;
GO


-- ======================================
-- Execute the following code on Server B
-- ======================================

-- Restore the Full Backup with NORECOVEY

RESTORE DATABASE DB1 FROM DISK = 'F:\BACKUP\DB1.bak' WITH NORECOVERY;
GO

RESTORE DATABASE DB2 FROM DISK = 'F:\BACKUP\DB2.bak' WITH NORECOVERY;
GO

RESTORE DATABASE DB3 FROM DISK = 'F:\BACKUP\DB3.bak' WITH NORECOVERY;
GO

RESTORE DATABASE DB4 FROM DISK = 'F:\BACKUP\DB4.bak' WITH NORECOVERY;
GO

RESTORE DATABASE DB5 FROM DISK = 'F:\BACKUP\DB5.bak' WITH NORECOVERY;
GO


-- Restore the log backup with NORECOVERY

RESTORE LOG DB1 FROM DISK = 'F:\BACKUP\DB1.trn' WITH NORECOVERY;
GO

RESTORE LOG DB2 FROM DISK = 'F:\BACKUP\DB2.trn' WITH NORECOVERY;
GO

RESTORE LOG DB3 FROM DISK = 'F:\BACKUP\DB3.trn' WITH NORECOVERY;
GO

RESTORE LOG DB4 FROM DISK = 'F:\BACKUP\DB4.trn' WITH NORECOVERY;
GO

RESTORE LOG DB5 FROM DISK = 'F:\BACKUP\DB5.trn' WITH NORECOVERY;
GO



-- Move the databases into the Availability Group

ALTER DATABASE DB1 SET HADR AVAILABILITY GROUP = AG;
GO

ALTER DATABASE DB2 SET HADR AVAILABILITY GROUP = AG;
GO

ALTER DATABASE DB3 SET HADR AVAILABILITY GROUP = AG;
GO

ALTER DATABASE DB4 SET HADR AVAILABILITY GROUP = AG;
GO

ALTER DATABASE DB5 SET HADR AVAILABILITY GROUP = AG;
GO


-- ======================================
-- Execute the following code on Server C
-- ======================================

-- Restore the Full Backup with NORECOVEY

RESTORE DATABASE DB1 FROM DISK = 'F:\BACKUP\DB1.bak' WITH NORECOVERY;
GO

RESTORE DATABASE DB2 FROM DISK = 'F:\BACKUP\DB2.bak' WITH NORECOVERY;
GO

RESTORE DATABASE DB3 FROM DISK = 'F:\BACKUP\DB3.bak' WITH NORECOVERY;
GO

RESTORE DATABASE DB4 FROM DISK = 'F:\BACKUP\DB4.bak' WITH NORECOVERY;
GO

RESTORE DATABASE DB5 FROM DISK = 'F:\BACKUP\DB5.bak' WITH NORECOVERY;
GO


-- Restore the log backup with NORECOVERY

RESTORE LOG DB1 FROM DISK = 'F:\BACKUP\DB1.trn' WITH NORECOVERY;
GO

RESTORE LOG DB2 FROM DISK = 'F:\BACKUP\DB2.trn' WITH NORECOVERY;
GO

RESTORE LOG DB3 FROM DISK = 'F:\BACKUP\DB3.trn' WITH NORECOVERY;
GO

RESTORE LOG DB4 FROM DISK = 'F:\BACKUP\DB4.trn' WITH NORECOVERY;
GO

RESTORE LOG DB5 FROM DISK = 'F:\BACKUP\DB5.trn' WITH NORECOVERY;
GO



-- Move the databases into the Availability Group

ALTER DATABASE DB1 SET HADR AVAILABILITY GROUP = AG;
GO

ALTER DATABASE DB2 SET HADR AVAILABILITY GROUP = AG;
GO

ALTER DATABASE DB3 SET HADR AVAILABILITY GROUP = AG;
GO

ALTER DATABASE DB4 SET HADR AVAILABILITY GROUP = AG;
GO

ALTER DATABASE DB5 SET HADR AVAILABILITY GROUP = AG;
GO
