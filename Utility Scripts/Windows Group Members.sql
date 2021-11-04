/*
Find All Windows Group Members
==============================
Author: Eitan Blumin
Date: 2021-11-04
*/

IF OBJECT_ID('tempdb..#GroupMembers') IS NOT NULL DROP TABLE #GroupMembers;
CREATE TABLE #GroupMembers (AccountName sysname, AccountType sysname, Privilege sysname, MappedName sysname, GroupPath sysname);
DECLARE @CurrentGroup sysname;

DECLARE Groups CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.server_principals
WHERE [type] = 'G'

OPEN Groups;

WHILE 1=1
BEGIN
	FETCH NEXT FROM Groups INTO @CurrentGroup;
	IF @@FETCH_STATUS <> 0 BREAK;

	BEGIN TRY

	INSERT INTO #GroupMembers
	EXEC master..xp_logininfo 
		@acctname = @CurrentGroup,
		@option = 'members';
	
	END TRY
	BEGIN CATCH
		PRINT N'Error while retrieving members of ' + @CurrentGroup + N'; ' + ERROR_MESSAGE()
	END CATCH
END

CLOSE Groups;
DEALLOCATE Groups;

SELECT *
FROM #GroupMembers
