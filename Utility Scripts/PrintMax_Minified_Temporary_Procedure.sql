IF OBJECT_ID('tempdb..#PrintMax') IS NOT NULL DROP PROC #PrintMax;
GO
/*
Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
Description:
This is a minified version of the PrintMax procedure (originally written by Ben Dill).
It's created as a temporary procedure.
*/
CREATE PROCEDURE #PrintMax @str NVARCHAR(MAX)
AS
BEGIN
 IF (@str IS NULL) RETURN;
 DECLARE @LBindex INT,@len INT;
 SET @len = 4000;
 WHILE (LEN(@str) > @len) BEGIN
  SET @LBindex = CHARINDEX((CHAR(10) + CHAR(13)) COLLATE database_default, REVERSE(LEFT(@str, @len)) COLLATE database_default);
  IF @LBindex = 0 AND @str COLLATE database_default LIKE N'%' + CHAR(13) + CHAR(10) + N'%'
    SET @LBindex = CHARINDEX((N',') COLLATE database_default, REVERSE(LEFT(@str, @len)) COLLATE database_default);
  PRINT LEFT(@str, @len - @LBindex + 1);
  SET @str = RIGHT(@str, LEN(@str) - @len + @LBindex - 1);
 END;
 IF (LEN(@str) > 0) PRINT @str;
END;
GO