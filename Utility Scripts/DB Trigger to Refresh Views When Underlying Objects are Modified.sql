/*
Database trigger to automatically refresh SQL Server views
==========================================================
Based on: https://www.mssqltips.com/sqlservertip/7598/refresh-sql-server-view-when-underlying-tables-are-modified/

Author: Eitan Blumin
Date: 2023-04-18
Description:
The following database trigger will automatically attempt to refresh views
whenever an underlying object is modified (table, function, another view).
It is fault-tolerant, meaning that it will not crash your original command
even if the view refresh fails.
*/
CREATE OR ALTER TRIGGER DBTR_FixViews ON DATABASE
FOR ALTER_TABLE, ALTER_VIEW, ALTER_FUNCTION
AS
BEGIN
  SET NOCOUNT ON;
  EXECUTE AS USER = 'dbo';
  DECLARE @EventData xml = EVENTDATA();
  DECLARE @sch  sysname = QUOTENAME(@EventData.value
                (N'(/EVENT_INSTANCE/SchemaName)[1]',  N'sysname')), 
          @obj  sysname = QUOTENAME(@EventData.value
                (N'(/EVENT_INSTANCE/ObjectName)[1]',  N'sysname')),
          @viewName nvarchar(550);
  
  DECLARE vws CURSOR
  LOCAL FAST_FORWARD
  FOR
    SELECT viewName = QUOTENAME(s.name) + N'.' + QUOTENAME(o.name)
    FROM sys.schemas AS s
    INNER JOIN sys.objects AS o
      ON s.[schema_id] = o.[schema_id]
    INNER JOIN sys.sql_expression_dependencies AS d
      ON o.[object_id] = d.referencing_id
    WHERE d.referenced_id = OBJECT_ID(@sch + N'.' + @obj)
      AND o.type = 'V';

  OPEN vws;

  WHILE 1=1
  BEGIN
	FETCH NEXT FROM vws INTO @viewName;
	IF @@FETCH_STATUS <> 0 BREAK;

	RAISERROR(N'Refreshing dependent view: %s',0,1,@viewName) WITH NOWAIT;

	BEGIN TRY
		EXEC sys.sp_refreshview @viewName;
	END TRY
	BEGIN CATCH
		DECLARE @errNum int, @errMsg nvarchar(MAX);
		SELECT @errNum = ERROR_NUMBER(), @errMsg = ERROR_MESSAGE();
		RAISERROR(N'Error %d: %s',0,1,@errNum,@errMsg);
	END CATCH
  END

  CLOSE vws;
  DEALLOCATE vws;
END
