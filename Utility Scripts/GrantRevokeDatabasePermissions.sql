USE MyDB;
GO

/*******************************************************************************************
Description:	Grant or revoke specified permissions to database principal (user or role).
Author:			Reut Almog Talmi @ Madeira Data Solutions
Created:		2023-06
Notes:			Can be executed localy on stand alone SQL instance, 
				on primary replica 
				or from secondary replica remotely using linked server to primary replica
********************************************************************************************/
CREATE OR ALTER PROCEDURE sp_DBA_GrantRevokeDatabasePermissions
	@PermissionState	NVARCHAR(50) = N'',
	@DBName				SYSNAME,
	@Permission			NVARCHAR(100),
	@Principal			NVARCHAR(100),
	@WithGrantOption	BIT = 0,
	@Debug				BIT = 0

AS


DECLARE	
	@SQLCommand				NVARCHAR(1000) = N'',
	@PrincipalReplicaName	SYSNAME	

	

IF @DBName IN (SELECT DISTINCT database_name FROM master.sys.dm_hadr_database_replica_cluster_states)
BEGIN
	SELECT @PrincipalReplicaName =	(
										SELECT primary_replica AS PrimaryReplicaName 
										FROM sys.dm_hadr_availability_group_states 
										WHERE synchronization_health = 2
										AND primary_replica != @@SERVERNAME		
									)
END
ELSE
BEGIN
	SELECT @PrincipalReplicaName = NULL
END


BEGIN	
	-- validate DB name 
	IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @DBName) 
	BEGIN
		RAISERROR('Database: %s doesn''t exist.',16,1,@DBName)
		RETURN
	END

	-- validate primary replica name is quoted
	IF (SELECT CHARINDEX(']',CHARINDEX('[',@PrincipalReplicaName))) = 0
	BEGIN
		SELECT @PrincipalReplicaName = QUOTENAME(@PrincipalReplicaName)
	END


	-- Validate permission state
	IF TRIM(@PermissionState) = '' OR @PermissionState IS NULL
	BEGIN
		RAISERROR ('Must declare ''GRANT'' or ''REVOKE'' for @PermissionState parameter.', 16,1) 
		RETURN
	END
	ELSE
	BEGIN
		SELECT @PermissionState = UPPER(TRIM(@PermissionState))

		
		SELECT @SQLCommand += 
			N'USE '+ QUOTENAME(@DBName)+ '; ' + @PermissionState +' ' + @Permission +	CASE
																							WHEN @PermissionState = 'GRANT'	 THEN + ' TO '
																							WHEN @PermissionState = 'REVOKE' THEN + ' FROM '
																						END
																					+ @Principal +	
																						CASE 
																							WHEN @WithGrantOption = 1 THEN + ' WITH GRANT OPTION;' 
																							ELSE ';' 
																						END
	
		
		IF @PrincipalReplicaName IS NOT NULL
		BEGIN
			SELECT @SQLCommand = CONCAT(N'EXEC (''', @SQLCommand ,''') AT ' , @PrincipalReplicaName, ';')
		END
		ELSE
		BEGIN
			SELECT @SQLCommand = @SQLCommand 
		END

		PRINT @SQLCommand

		IF @Debug != 1
		BEGIN
			EXEC sp_executesql @SQLCommand
		END
	
	END


END


GO



