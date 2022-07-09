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
-- Create asymmetric key from DLL
IF NOT EXISTS (SELECT * FROM master.sys.asymmetric_keys WHERE name = '$(CLRKeyName)')
	CREATE ASYMMETRIC KEY [$(CLRKeyName)]
	FROM EXECUTABLE FILE = '$(PathToSignedDLL)'
GO
-- Create server login from asymmetric key
IF NOT EXISTS (SELECT name FROM master.sys.syslogins WHERE name = '$(CLRLoginName)')
	CREATE LOGIN [$(CLRLoginName)] FROM ASYMMETRIC KEY [$(CLRKeyName)];
GO
-- Grant UNSAFE/EXTERNAL_ACCESS/SAFE ASSEMBLY permissions to login which was created from DLL signing key
GRANT UNSAFE ASSEMBLY TO [$(CLRLoginName)];
GO
-- Return execution context to intended target database
USE [$(DatabaseName)];
GO