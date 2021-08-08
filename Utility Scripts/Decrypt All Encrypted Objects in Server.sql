-- Must connect to SQL Server using the Dedicate Admin Connection, eg "admin:localhost". Verified with SQL Server 2012.
-- Originally from Williams Orellana's blog: http://williamsorellana.org/2012/02/decrypt-sql-stored-procedures/
-- Adapted from Jason Stangroome: https://gist.github.com/jstangroome/4020443
-- The results will be returned in a grid. Be sure to enable "Retain CR/LF on copy or save" (Tools > Options... > Query Results > SQL Server > Results to Grid)

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DROP TABLE IF EXISTS #Results;
CREATE TABLE #Results (DatabaseName sysname, SchemaName sysname, ObjectName sysname, ObjectType sysname NULL, ObjectDefinition nvarchar(max) NULL, DefinitionLength INT NULL);
DECLARE @Modules AS TABLE (DBName sysname, ObjectID INT);

DECLARE @DatabaseName SYSNAME;
DECLARE @ObjectOwnerOrSchema SYSNAME
DECLARE @ObjectName SYSNAME
DECLARE @spExecuteSQL NVARCHAR(1000)
DECLARE @CMD NVARCHAR(MAX)

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE HAS_DBACCESS([name]) = 1

OPEN DBs;
WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @DatabaseName;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @spExecuteSQL = QUOTENAME(@DatabaseName) + N'..sp_executesql'

	INSERT INTO @Modules(DBName,ObjectID)
	EXEC @spExecuteSQL N'SELECT DISTINCT DB_NAME(), id FROM syscomments WHERE encrypted = 1' WITH RECOMPILE;
END

CLOSE DBs;
DEALLOCATE DBs;

DECLARE @ObjectID INT;

DECLARE Obj CURSOR
LOCAL FAST_FORWARD
FOR
SELECT DBName, ObjectID FROM @Modules;

OPEN Obj;

WHILE 1=1
BEGIN
FETCH NEXT FROM Obj INTO @DatabaseName, @ObjectID
IF @@FETCH_STATUS <> 0 BREAK;

SET @ObjectOwnerOrSchema = OBJECT_SCHEMA_NAME(@ObjectID, DB_ID(@DatabaseName))
SET @ObjectName = OBJECT_NAME(@ObjectID, DB_ID(@DatabaseName))
SET @spExecuteSQL = QUOTENAME(@DatabaseName) + N'..sp_executesql'

DECLARE @i INT
DECLARE @ObjectDataLength INT
DECLARE @ContentOfEncryptedObject NVARCHAR(MAX)
DECLARE @ContentOfDecryptedObject VARCHAR(MAX)
DECLARE @ContentOfFakeObject NVARCHAR(MAX)
DECLARE @ContentOfFakeEncryptedObject NVARCHAR(MAX)
DECLARE @ObjectType NVARCHAR(128)

-- Determine the type of the object
IF OBJECT_ID(QUOTENAME(@DatabaseName) + N'.' + QUOTENAME(@ObjectOwnerOrSchema) + '.' + QUOTENAME(@ObjectName), 'PROCEDURE') IS NOT NULL
SET @ObjectType = 'PROCEDURE'
ELSE
IF OBJECT_ID(QUOTENAME(@DatabaseName) + N'.' + QUOTENAME(@ObjectOwnerOrSchema) + '.' + QUOTENAME(@ObjectName), 'TRIGGER') IS NOT NULL
SET @ObjectType = 'TRIGGER'
ELSE
IF OBJECT_ID(QUOTENAME(@DatabaseName) + N'.' + QUOTENAME(@ObjectOwnerOrSchema) + '.' + QUOTENAME(@ObjectName), 'VIEW') IS NOT NULL
SET @ObjectType = 'VIEW'
ELSE
SET @ObjectType = 'FUNCTION'

-- Get the binary representation of the object- syscomments no longer holds
-- the content of encrypted object.
SET @CMD = N'
SELECT TOP 1 @ContentOfEncryptedObject = imageval
FROM sys.sysobjvalues
WHERE objid = @ObjectID
AND valclass = 1 and subobjid = 1'

EXEC @spExecuteSQL @CMD, N'@ContentOfEncryptedObject NVARCHAR(MAX) OUTPUT, @ObjectID INT', @ContentOfEncryptedObject OUTPUT, @ObjectID;

SET @ObjectDataLength = DATALENGTH(@ContentOfEncryptedObject)/2

-- We need to alter the existing object and make it into a dummy object
-- in order to decrypt its content. This is done in a transaction
-- (which is later rolled back) to ensure that all changes have a minimal
-- impact on the database.
SET @ContentOfFakeObject = N'ALTER ' + @ObjectType + N' ' + QUOTENAME(@ObjectOwnerOrSchema) + '.' + QUOTENAME(@ObjectName) + N' WITH ENCRYPTION AS'

WHILE DATALENGTH(@ContentOfFakeObject)/2 < @ObjectDataLength
BEGIN
IF DATALENGTH(@ContentOfFakeObject)/2 + 8000 < @ObjectDataLength
SET @ContentOfFakeObject = @ContentOfFakeObject + REPLICATE(N'-', 8000)
ELSE
SET @ContentOfFakeObject = @ContentOfFakeObject + REPLICATE(N'-', @ObjectDataLength - (DATALENGTH(@ContentOfFakeObject)/2))
END

-- Since we need to alter the object in order to decrypt it, this is done
-- in a transaction
SET XACT_ABORT OFF

BEGIN TRY

BEGIN TRAN

EXEC @spExecuteSQL @ContentOfFakeObject

IF @@ERROR <> 0
ROLLBACK TRAN

-- Get the encrypted content of the new "fake" object.
SET @CMD = N'
SELECT TOP 1 @ContentOfFakeEncryptedObject = imageval
FROM sys.sysobjvalues
WHERE objid = @ObjectID
AND valclass = 1 and subobjid = 1'

EXEC @spExecuteSQL @CMD, N'@ContentOfFakeEncryptedObject NVARCHAR(MAX) OUTPUT, @ObjectID INT', @ContentOfFakeEncryptedObject OUTPUT, @ObjectID;

IF @@TRANCOUNT > 0
ROLLBACK TRAN

-- Generate a CREATE script for the dummy object text.
SET @ContentOfFakeObject = N'CREATE ' + @ObjectType + N' ' +QUOTENAME(@ObjectOwnerOrSchema) + '.' + QUOTENAME(@ObjectName) + N' WITH ENCRYPTION AS'

WHILE DATALENGTH(@ContentOfFakeObject)/2 < @ObjectDataLength
BEGIN
IF DATALENGTH(@ContentOfFakeObject)/2 + 8000 < @ObjectDataLength
SET @ContentOfFakeObject = @ContentOfFakeObject + REPLICATE(N'-', 8000)
ELSE
SET @ContentOfFakeObject = @ContentOfFakeObject + REPLICATE(N'-', @ObjectDataLength - (DATALENGTH(@ContentOfFakeObject)/2))
END

SET @i = 1

--Fill the variable that holds the decrypted data with a filler character
SET @ContentOfDecryptedObject = N''

WHILE DATALENGTH(@ContentOfDecryptedObject)/2 < @ObjectDataLength
BEGIN
IF DATALENGTH(@ContentOfDecryptedObject)/2 + 8000 < @ObjectDataLength
SET @ContentOfDecryptedObject = @ContentOfDecryptedObject + REPLICATE(N'A', 8000)
ELSE
SET @ContentOfDecryptedObject = @ContentOfDecryptedObject + REPLICATE(N'A', @ObjectDataLength - (DATALENGTH(@ContentOfDecryptedObject)/2))
END

WHILE @i <= @ObjectDataLength BEGIN
--xor real & fake & fake encrypted
SET @ContentOfDecryptedObject = STUFF(@ContentOfDecryptedObject, @i, 1,
NCHAR(
UNICODE(SUBSTRING(@ContentOfEncryptedObject, @i, 1)) ^
(
UNICODE(SUBSTRING(@ContentOfFakeObject, @i, 1)) ^
UNICODE(SUBSTRING(@ContentOfFakeEncryptedObject, @i, 1))
)))

SET @i = @i + 1
END

END TRY
BEGIN CATCH
	PRINT N'Error decrypting ' + @ObjectType + N' ' +QUOTENAME(@DatabaseName) + N'.' +QUOTENAME(@ObjectOwnerOrSchema) + '.' + QUOTENAME(@ObjectName) + N':'
	PRINT ERROR_MESSAGE()

	SET @ContentOfDecryptedObject = NULL
END CATCH

INSERT INTO #Results
VALUES(@DatabaseName, @ObjectOwnerOrSchema, @ObjectName, @ObjectType, @ContentOfDecryptedObject, @ObjectDataLength)
END

CLOSE Obj;
DEALLOCATE Obj;

-- output the content of the decrypted object
SELECT DatabaseName, SchemaName, ObjectName, ObjectType
, DecryptedDefinition = SUBSTRING(ObjectDefinition, 1, DefinitionLength)
--, DecryptedDefinitionXML = (SELECT SUBSTRING(ObjectDefinition, 1, DefinitionLength) FOR XML PATH(''), TYPE)
FROM #Results
