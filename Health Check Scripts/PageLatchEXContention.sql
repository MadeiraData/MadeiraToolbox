SET NOCOUNT ON 
DECLARE @dbname SYSNAME, @dbid INT, @objectid INT, @indexid INT, @indexname SYSNAME, @sql VARCHAR(8000), @manul_identification VARCHAR(8000)

IF (CONVERT(int, SERVERPROPERTY('ProductMajorVersion')) >= 15)
BEGIN

    DROP TABLE IF EXISTS #PageLatchEXContention

    SELECT DB_NAME(page_info.database_id) DbName, r.db_id DbId, page_info.[object_id] ObjectId, page_info.index_id IndexId
    INTO #PageLatchEXContention
    FROM sys.dm_exec_requests AS er
        CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) AS st 
        CROSS APPLY sys.fn_PageResCracker (er.page_resource) AS r  
        CROSS APPLY sys.dm_db_page_info(r.[db_id], r.[file_id], r.page_id, 'DETAILED') AS page_info
    WHERE er.wait_type = 'PAGELATCH_EX' AND page_info.database_id NOT IN (DB_ID('master'),DB_ID('msdb'), DB_ID('model'), DB_ID('tempdb')) 
    GROUP BY page_info.database_id, r.db_id, page_info.[object_id], page_info.index_id
    HAVING COUNT(er.session_id) > 5 AND MAX (er.wait_time) > 10

    SELECT * FROM #PageLatchEXContention 
    IF EXISTS (SELECT 1 FROM #PageLatchEXContention) 
    BEGIN
        DECLARE optimize_for_seq_key_cursor CURSOR FOR
            SELECT DbName, DbId, ObjectId, IndexId FROM #PageLatchEXContention 
            
        OPEN optimize_for_seq_key_cursor
        FETCH NEXT FROM optimize_for_seq_key_cursor into @dbname, @dbid, @objectid , @indexid 
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SELECT 'Consider using below statement to enable OPTIMIZE_FOR_SEQUENTIAL_KEY for the indexes in the "' + @dbname + '" database' AS Recommendation
            SELECT @sql =  'select ''use ' + @dbname + '; ALTER INDEX '' + i.name + '' ON ' + OBJECT_NAME(@objectid, @dbid) + ' SET (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON )'' AS Corrective_Action from #PageLatchEXContention pl JOIN ' + @dbname+'.sys.indexes i ON pl.ObjectID = i.object_id WHERE object_id = ' + CONVERT(VARCHAR, @objectid) + ' AND index_id = ' + CONVERT(VARCHAR, @indexid)

            EXECUTE (@sql) 
            FETCH NEXT FROM optimize_for_seq_key_cursor INTO @dbname, @dbid, @objectid , @indexid 

        END

        CLOSE optimize_for_seq_key_cursor
        DEALLOCATE optimize_for_seq_key_cursor
    
    END
    ELSE
        SELECT 'No PAGELATCH_EX contention found on user databases on in SQL Server at this time'
END
ELSE
BEGIN
    
    IF OBJECT_ID('tempdb..#PageLatchEXContentionLegacy') IS NOT NULL
        DROP TABLE #PageLatchEXContentionLegacy
    
    SELECT 'dbcc traceon (3604); dbcc page(' + replace(wait_resource,':',',') + ',3); dbcc traceoff (3604)' TSQL_Command
    INTO #PageLatchEXContentionLegacy
    FROM sys.dm_exec_requests er
    WHERE er.wait_type = 'PAGELATCH_EX' AND er.database_id NOT IN (db_id('master'),db_id('msdb'), db_id('model'), db_id('tempdb')) 
    GROUP BY wait_resource
    HAVING COUNT(er.session_id) > 5 AND Max (er.wait_time) > 10

    SELECT * FROM #PageLatchEXContentionLegacy
    
    IF EXISTS(SELECT 1 FROM #PageLatchEXContentionLegacy)
    BEGIN
        SELECT 'On SQL Server 2017 or lower versions, you can manually identify the object where contention is occurring using DBCC PAGE locate the m_objId = ??. Then SELECT OBJECT_NAME(object_id_identified) and locate indexes with sequential values in this object' AS Recommendation
        
        DECLARE get_command CURSOR FOR
            SELECT TSQL_Command from #PageLatchEXContentionLegacy 

        OPEN get_command
        FETCH NEXT FROM get_command into @sql
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SELECT @sql AS Step1_Run_This_Command_To_Find_Object
            SELECT 'select OBJECT_NAME(object_id_identified)' AS Step2_Find_Object_Name_From_ID
            FETCH NEXT FROM get_command INTO @sql
        END

        CLOSE get_command
        DEALLOCATE get_command

        SELECT 'Follow https://learn.microsoft.com/troubleshoot/sql/performance/resolve-pagelatch-ex-contention for resolution recommendations that fits your environment best' Step3_Apply_KB_article
        
    END
    ELSE
        SELECT 'No PAGELATCH_EX contention found on user databases on in SQL Server at this time'

END