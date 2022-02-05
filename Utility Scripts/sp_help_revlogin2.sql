IF OBJECT_ID('tempdb..#sp_help_revlogin2') IS NOT NULL DROP PROCEDURE #sp_help_revlogin2
GO
/*********************************************************************************************
sp_help_revlogin2 V1.2
Eitan Blumin

https://eitanblumin.com | https://madeiradata.com
https://gist.github.com/EitanBlumin/1f19b0b3f59a9220641c559653b90f15
https://github.com/MadeiraData/MadeiraToolbox/blob/master/Utility%20Scripts/sp_help_revlogin2.sql
https://eitanblumin.com/2021/05/11/t-sql-tuesday-138-sp_help_revlogin-is-dead-long-live-sp_help_revlogin2/

This is a simpler alternative to sp_help_revlogin.

Standard disclaimer: You use scripts off of the web at your own risk.  I fully expect this
     script to work without issue but I've been known to be wrong before.
    
Parameters:
    @login_name
	Optionally filter for a specific login name. Defaults to NULL (all logins).

    @include_system_logins
        If set to 1, will output system principals such as sa, NT SERVICE accounts, and ##... accounts.

    @command_separator
        By default equals to 'GO'. Will be used as a separator between each CREATE LOGIN command.
*********************************************************************************************
-- V1.2
-- 14/12/2021 - added support for Azure SQL DB

-- V1.1
-- 23/06/2021 - added new optional parameter @login_name

-- V1.0
-- 05/05/2021
*********************************************************************************************/
CREATE PROCEDURE #sp_help_revlogin2
	@login_name sysname = NULL,
	@include_system_logins bit = 0,
	@command_separator nvarchar(1000) = N'GO'
AS
SET NOCOUNT, ARITHABORT, XACT_ABORT, QUOTED_IDENTIFIER ON;

DECLARE @Output AS TABLE (Content NVARCHAR(MAX));
PRINT N'
/***************************************************/
/***          sp_help_revlogin2 output           ***/
/***************************************************/
-- Generated on: ' + CONVERT(nvarchar(25), GETDATE(),121)

IF CONVERT(int, SERVERPROPERTY('EngineEdition')) <> 5 AND OBJECT_ID('sys.server_principals') IS NOT NULL
BEGIN
  PRINT N'-- Generating from: sys.server_principals'

  INSERT INTO @Output
  SELECT
   + CHAR(13) + CHAR(10) + N'-- Login: ' + [name] + CHAR(13) + CHAR(10)
   + CASE WHEN type IN ( 'G', 'U')
     THEN N'CREATE LOGIN ' + QUOTENAME( login_name ) + CHAR(13) + CHAR(10) + ' FROM WINDOWS WITH DEFAULT_DATABASE = ' + QUOTENAME( ISNULL(default_database_name, DB_NAME()) )
     ELSE N'CREATE LOGIN ' + QUOTENAME( login_name ) + CHAR(13) + CHAR(10) + ' WITH PASSWORD = ' + CONVERT(nvarchar(max), CAST( LOGINPROPERTY( login_name, 'PasswordHash' ) AS varbinary (max)), 1)
  	+ ' HASHED, SID = ' +  CONVERT(nvarchar(max), [sid], 1) + CHAR(13) + CHAR(10) + ', DEFAULT_DATABASE = ' + QUOTENAME( ISNULL(default_database_name, DB_NAME()) )
   END
   + CASE WHEN CAST(LOGINPROPERTY( login_name, 'HistoryLength' ) AS int) <> 0 THEN N', CHECK_POLICY = ON' ELSE N'' END
   + CASE WHEN LOGINPROPERTY( login_name, 'DaysUntilExpiration' ) IS NOT NULL THEN N', CHECK_EXPIRATION = ON' ELSE N'' END
   + N';'
   + CASE WHEN dp.is_disabled = 1 THEN CHAR(13) + CHAR(10) + N'ALTER LOGIN ' + QUOTENAME( login_name ) + N' DISABLE;' ELSE N'' END
  FROM sys.server_principals AS dp
  CROSS APPLY ( SELECT [name] AS login_name ) AS l
  WHERE [sid] IS NOT NULL
  AND type IN ( 'S', 'G', 'U' )
  AND (@login_name IS NULL OR @login_name = l.login_name)
  AND (
      @include_system_logins = 1
      OR ([sid] NOT IN (0x00, 0x01) AND [name] NOT LIKE N'##%##' AND [name] NOT LIKE N'NT SERVICE\%' AND [name] NOT LIKE N'NT AUTHORITY\%')
      )
END


IF OBJECT_ID('sys.sql_logins') IS NOT NULL AND CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5
BEGIN
  PRINT N'-- Generating from: sys.sql_logins'

  INSERT INTO @Output
  SELECT
   + CHAR(13) + CHAR(10) + N'-- Login: ' + [name] + CHAR(13) + CHAR(10)
   + CASE WHEN type IN ( 'G', 'U')
     THEN N'CREATE LOGIN ' + QUOTENAME( login_name ) + CHAR(13) + CHAR(10) + ' FROM WINDOWS WITH DEFAULT_DATABASE = ' + QUOTENAME( ISNULL(default_database_name, DB_NAME()) )
     ELSE N'CREATE LOGIN ' + QUOTENAME( login_name ) + CHAR(13) + CHAR(10) + ' WITH PASSWORD = ' + CONVERT(nvarchar(max), dp.password_hash, 1)
  	+ ' HASHED, SID = ' +  CONVERT(nvarchar(max), [sid], 1) + CHAR(13) + CHAR(10) + ', DEFAULT_DATABASE = ' + QUOTENAME( ISNULL(default_database_name, DB_NAME()) )
   END
   + CASE WHEN CAST(LOGINPROPERTY( login_name, 'HistoryLength' ) AS int) <> 0 THEN N', CHECK_POLICY = ON' ELSE N'' END
   + CASE WHEN LOGINPROPERTY( login_name, 'DaysUntilExpiration' ) IS NOT NULL THEN N', CHECK_EXPIRATION = ON' ELSE N'' END
   + N';'
   + CASE WHEN dp.is_disabled = 1 THEN CHAR(13) + CHAR(10) + N'ALTER LOGIN ' + QUOTENAME( login_name ) + N' DISABLE;' ELSE N'' END
  FROM sys.sql_logins AS dp
  CROSS APPLY ( SELECT [name] AS login_name ) AS l
  WHERE [sid] IS NOT NULL
  AND type IN ( 'S', 'G', 'U' )
  AND (@login_name IS NULL OR @login_name = l.login_name)
  AND (
      @include_system_logins = 1
      OR ([sid] NOT IN (0x00, 0x01) AND [name] NOT LIKE N'##%##' AND [name] NOT LIKE N'NT SERVICE\%' AND [name] NOT LIKE N'NT AUTHORITY\%')
      )
END
ELSE IF CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5
BEGIN
	RAISERROR(N'This script does not support Azure SQL User Databases. You must run this from the "master" database.',16,1);
END

IF CONVERT(int, SERVERPROPERTY('EngineEdition')) <> 5 AND NOT EXISTS (SELECT NULL FROM @Output)
BEGIN
  PRINT N'-- Generating from: sys.database_principals'

  INSERT INTO @Output	
  SELECT
   + N'-- Login: ' + [name] + CHAR(13) + CHAR(10)
   + CASE WHEN type IN ( 'G', 'U')
     THEN N'CREATE LOGIN ' + QUOTENAME( [name] ) + CHAR(13) + CHAR(10) + ' FROM WINDOWS WITH DEFAULT_DATABASE = ' + QUOTENAME( ISNULL(CONVERT(sysname, LOGINPROPERTY( [name], 'DefaultDatabase')), DB_NAME()) )
     ELSE N'CREATE LOGIN ' + QUOTENAME( [name] ) + CHAR(13) + CHAR(10) + ' WITH PASSWORD = ' + CONVERT(nvarchar(max), CAST( LOGINPROPERTY( [name], 'PasswordHash' ) AS varbinary (max)), 1)
  	+ ' HASHED, SID = ' +  CONVERT(nvarchar(max), [sid], 1) + CHAR(13) + CHAR(10) + ', DEFAULT_DATABASE = ' + QUOTENAME( ISNULL(CONVERT(sysname, LOGINPROPERTY( [name], 'DefaultDatabase')), DB_NAME()) )
   END
   + CASE WHEN CAST(LOGINPROPERTY( [name], 'HistoryLength' ) AS int) <> 0 THEN N', CHECK_POLICY = ON' ELSE N'' END
   + CASE WHEN LOGINPROPERTY( [name], 'DaysUntilExpiration' ) IS NOT NULL THEN N', CHECK_EXPIRATION = ON' ELSE N'' END
   + N';'
   --, UserCreateScript = N'CREATE USER ' + QUOTENAME([name]) + N' FOR LOGIN ' + QUOTENAME( [name] ) + N';'
  FROM sys.database_principals AS dp
  WHERE [sid] IS NOT NULL
  AND type IN ( 'S', 'G', 'U' )
  AND (@login_name IS NULL OR @login_name = [name])
  AND (
      @include_system_logins = 1
      OR ([sid] NOT IN (0x00, 0x01) AND [name] NOT LIKE N'##%##')
      )
END

DECLARE @Content NVARCHAR(MAX)

DECLARE Outputs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT Content FROM @Output

OPEN Outputs

WHILE 1=1
BEGIN
	FETCH NEXT FROM Outputs INTO @Content;
	IF @@FETCH_STATUS <> 0
		BREAK;
	
	PRINT ISNULL(@command_separator, CHAR(13) + CHAR(10))
	PRINT @Content;
END

CLOSE Outputs
DEALLOCATE Outputs

PRINT ISNULL(@command_separator, CHAR(13) + CHAR(10))
GO


EXEC #sp_help_revlogin2
	-- @include_system_logins = 1
