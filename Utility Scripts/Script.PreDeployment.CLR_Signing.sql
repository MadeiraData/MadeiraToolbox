/*
 Pre-Deployment Script Template	For Importing a Signed CLR Assembly (SSDT Project)
--------------------------------------------------------------------------------------
In order to use this script, you must configure the following SQLCMD Variables in your project:

$(PathToSignedDLL)
$(CLRKeyName)
$(CLRLoginName)

To configure your SQLCMD Variables: Right-click on your DB project, select "Properties", and go to "SQLCMD Variables".
Add your new SQLCMD Variables in the grid.

The $(PathToSignedDLL) variable is particularly tricky, because it requires you to know, in advance,
the path to your signed assembly DLL. That file may not be available until after you first build your project.
Consistent file paths must be maintained, especially when using CI/CD pipelines.

Each signature "thumbprint" may only be imported once into the SQL Server.
It wouldn't be possible to import it more than once even if the symmetric keys are named differently.
However, the same key can be used to sign multiple assemblies.
So, you should use that to your advantage.

To sign your assembly:
Right-click on your DB project, select "Properties", and go to "SQLCLR".
Click on the "Signing..." button.
Enable the "Sign the assembly" checkbox.
Using the dropdown box, choose an existing strong name key file (*.snk) or create a new one.
Set a password for the key file.
This password is required for this phase only (initial signing config).
Make sure to save your password, especially if you plan to use the same key file for other assemblies.
--------------------------------------------------------------------------------------
*/
-- Make sure clr is enabled
IF EXISTS (select * from sys.configurations where name IN ('clr enabled') and value_in_use = 0)
BEGIN
	DECLARE @InitAdvanced INT;
	SELECT @InitAdvanced = CONVERT(int, value) FROM sys.configurations WHERE name = 'show advanced options';

	IF @InitAdvanced = 0
	BEGIN
		EXEC sp_configure 'show advanced options', 1;
		RECONFIGURE;
	END

	EXEC sp_configure 'clr enabled', 1;
	RECONFIGURE;

	IF @InitAdvanced = 0
	BEGIN
		EXEC sp_configure 'show advanced options', 0;
		RECONFIGURE;
	END
END
GO
-- Database context must be switched to [master] when creating the key and login
use [master];
GO
IF NOT EXISTS (select * from sys.asymmetric_keys WHERE name = '$(CLRKeyName)')
BEGIN
	BEGIN TRY
		PRINT N'Creating encryption key from: $(PathToSignedDLL)'
		CREATE ASYMMETRIC KEY [$(CLRKeyName)]
		FROM EXECUTABLE FILE = '$(PathToSignedDLL)'
	END TRY
	BEGIN CATCH
		IF ERROR_NUMBER() = 15396
		BEGIN
			RAISERROR(N'An encryption key with the same thumbprint was already created in this database with a different name.', 0,1);
			IF EXISTS(
				SELECT *
				FROM sys.asymmetric_keys AS ak
				LEFT JOIN sys.syslogins AS l ON l.sid = ak.sid
				WHERE l.sid IS NULL
			)
			BEGIN
				RAISERROR(N'Looks like there is no login for the existing encryption key. Please create one manually!', 11,1);
			END
		END
		ELSE
		BEGIN
			THROW;
		END
	END CATCH
END
GO
IF NOT EXISTS (select name from sys.syslogins where name = '$(CLRLoginName)')
AND EXISTS (select * from sys.asymmetric_keys WHERE name = '$(CLRKeyName)')
BEGIN
	PRINT N'Creating login from encryption key...'
	CREATE LOGIN [$(CLRLoginName)] FROM ASYMMETRIC KEY [$(CLRKeyName)];
END
GO
IF EXISTS (select name from sys.syslogins where name = '$(CLRLoginName)')
BEGIN
	GRANT UNSAFE ASSEMBLY TO [$(CLRLoginName)];
END
GO
-- Return execution context to intended target database
USE [$(DatabaseName)];
GO