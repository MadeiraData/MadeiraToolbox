/*===========================================================================================
************Active Geo-Replication Setup************

 Author: Eric Rouach, Madeira Data Solutions
 Create date: July 2023
 Description:
 
 The following steps describe how to setup Active Geo Replication for an Azure SQL Database.
 In other words, how to create a read-only replica of any Azure SQL Database.

 Pre-requisites: An existing Azure SQL Database and a dedicated Azure SQL (logical) Server
 to which the read-only secondary replica will connect
===========================================================================================*/

--1.Run the following script on the “master” database in the primary (source) server:
CREATE LOGIN geodrsetup WITH PASSWORD = 'StrongPassword!@#';
CREATE USER geodrsetup FOR LOGIN geodrsetup;
ALTER ROLE dbmanager ADD MEMBER geodrsetup;

SELECT sid FROM sys.sql_logins WHERE [name] = 'geodrsetup';

--2.Take note of the SID of the new login returned from the script above. 
--Copy it to the script below, and run it on the “master” database in the secondary (target) server:
CREATE LOGIN geodrsetup WITH PASSWORD = 'StrongPassword!', SID = 0x123456789123456789;
CREATE USER geodrsetup FOR LOGIN geodrsetup;
ALTER ROLE dbmanager ADD MEMBER geodrsetup;

--3.Run the following script on the user database in the primary (source) server:
CREATE USER geodrsetup FOR LOGIN geodrsetup;
ALTER ROLE db_owner ADD MEMBER geodrsetup;

--4.Connect as “geodrsetup” to the “master” database in the primary (source) server, 
--and run the script below after replacing the user database name and secondary server name:
ALTER DATABASE [YourDatabase] ADD SECONDARY ON SERVER [secondaryserver.database.windows.net];

--5.Run the query below to track the progress of the replication:
SELECT * FROM sys.dm_operation_status;

--6.Once the progress has reached the "COMPLETED" status, you're done!