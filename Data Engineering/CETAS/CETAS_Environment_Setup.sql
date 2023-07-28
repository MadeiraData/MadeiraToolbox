/*##############################################
------------Setup CETAS Environment for SQL Server 2022------------

Written by: Eric Rouach, Madeira Data Solutions 
Last Modified Date: 2023-07-28

Description:

Create External Table As Select or "CETAS" has finally become available on SQL Server with the release of the 2022
version. 
After a short setup, we can create various formats files containing any query's result set. 
The created file/s must be kept on an Azure storage solution i.e. Azure Blob Storage.
The process also creates an external table reflecting the current file's content.

You may find a detailed article about CETAS here:
https://www.madeiradata.com/post/cetas-in-sql-server-2022

Here's how to setup your environment:

(Useful documentation links are also provided)
##############################################*/


--One-time set up:

--In case you haven't selected Polybase while installing SQL Server 2022, go to SQL Server Installation Center 
--and add the missing feature.
/*
https://learn.microsoft.com/en-us/sql/database-engine/install-windows/add-features-to-an-instance-of-sql-server-setup?view=sql-server-ver16
*/
-- make sure Polybase is installed:
SELECT SERVERPROPERTY ('IsPolyBaseInstalled') AS IsPolyBaseInstalled;
--In case the above query returned 0, run the script below: 
EXEC sp_configure @configname = 'polybase enabled', @configvalue = 1;
RECONFIGURE;
GO

--You will also need to enable the "allow polybase export" configuration option:
sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO
sp_configure 'allow polybase export', 1;
GO
RECONFIGURE;
GO

--The next step is the creation of 4 external objects

--1) Create MASTER KEY
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'StrongPassword123!' ;
GO

--2) Create DATABASE SCOPED CREDENTIAL
-- when using a blob storage as a target location, use SAS as the identity
-- The SECRET below is the SAS token that you must generate from the blob storage container
-- in the Azure Portal. Before generating the token, make sure to grant the READ, WRITE and CREATE permissions 

CREATE DATABASE SCOPED CREDENTIAL DatabaseScopedCredential1 --I like naming objects as simply as possible 
WITH
  IDENTITY = 'SHARED ACCESS SIGNATURE', 
  --below is an SAS token example, replace it with your own
  SECRET = 'sp=racwdli&st=2023-05-02T10:56:07Z&se=2023-12-31T19:56:07Z&spr=https&sv=2021-12-02&sr=c&sig=NBLOfYa7G9COXq%2FVAMI839lA6W01SAKQEVZIvEZFzPg%3D';
GO

--3) Create EXTERNAL DATA SOURCE
CREATE EXTERNAL DATA SOURCE ExternalDataSource1
WITH
  ( 
    -- "LOCATION" is the container's URL
	LOCATION = 'abs://cetas.blob.core.windows.net/test1',
    CREDENTIAL = DatabaseScopedCredential1
  );
GO

--4) Create EXTERNAL FILE FORMAT
CREATE EXTERNAL FILE FORMAT ExternalFileFormat1
    WITH (FORMAT_TYPE = PARQUET); -- <==== in this case, I chose the .parquet file format 
GO
/* check supported file formats
https://learn.microsoft.com/en-us/sql/t-sql/statements/create-external-file-format-transact-sql?view=sql-server-ver16&tabs=delimited
*/
-- check objects creation

select * from sys.symmetric_keys
select * from sys.database_scoped_credentials
select * from sys.external_data_sources
select * from sys.external_file_formats

-- cleanup objects
--DROP EXTERNAL FILE FORMAT ExternalFileFormat1
--DROP EXTERNAL DATA SOURCE ExternalDataSource1
--DROP DATABASE SCOPED CREDENTIAL DatabaseScopedCredential1
--DROP MASTER KEY

GO

/*
https://learn.microsoft.com/en-us/sql/t-sql/statements/create-external-table-as-select-transact-sql?view=sql-server-ver16&tabs=powershell
*/
--####################################################################