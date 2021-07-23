/*
========================================================
Setup your Azure Sql database for Cross-Database queries
========================================================

The following commands create the required objects needed for being able to perform
"cross-database" queries in Microsoft Azure SQL.

Once those objects are created, you will be able to easily transfer data 
from one "source "database to another "target" database.

The following scripts must be run in the "target" database.
*/


--1) Create a master key:

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Strongpassword123'
--check the master key has been created (optional):
SELECT * FROM sys.symmetric_keys

--2) Create a Database Scoped Credential:

CREATE DATABASE SCOPED CREDENTIAL DatabaseScopedCredentialName	
WITH IDENTITY = 'yourlogin',	--the IDENTITY is the Login Name we used to login to the Server.
	 SECRET   = 'yourpassword';	--the SECRET is the Password we used to login to the Server.

--3) Create an External Data Source:

CREATE EXTERNAL DATA SOURCE ExternalDataSourceName
WITH
(
	TYPE             = RDBMS, --Relational Database Management System
	LOCATION         = 'yourserver.database.windows.net', -- your Azure server name.
	DATABASE_NAME    = 'SourceDatbaseName', -- the database we use as our data source.
	CREDENTIAL       = DatabaseScopedCredentialName -- the name we gave to our DATABASE SCOPED CREDENTIAL.
)
;

--4) Create an External Table:

CREATE EXTERNAL TABLE [schema].[SourceTableName_Ext] -- I recommentd adding the "_Ext" to the source table name
	(
		Col1      INT                 ,
		Col2      VARCHAR(20) NOT NULL,
		Col3      VARCHAR(30) NOT NULL,
		Col4      VARCHAR(12) NOT NULL,
		Col5      VARCHAR(60) NOT NULL,
		Col6      INT
	)
WITH
	(
		DATA_SOURCE = ExternalDataSourceName,
		SCHEMA_NAME = 'SchemaName',
		OBJECT_NAME = 'SourceTableName'
	)
;

-- Now you can use the external table as any other table within the same database!

/*
Written by: Eric Rouach, Madeira Data Solutions - 2021
eric@madeiradata.com
*/
