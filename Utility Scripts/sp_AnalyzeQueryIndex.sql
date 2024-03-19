/*
This procedure, when executed against a problematic SQL query, provides insights into missing indexes that could improve query performance.
Source: https://techcommunity.microsoft.com/t5/azure-database-support-blog/lesson-learned-481-query-performance-analysis-tips/ba-p/4088795
*/
CREATE PROCEDURE sp_AnalyzeQueryIndex
    @SQLQuery NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @TableNames TABLE (
        SourceSchema NVARCHAR(128),
        TableName NVARCHAR(128),
        ObjectId INT
    );

    INSERT INTO @TableNames (SourceSchema, TableName, ObjectId)
    SELECT DISTINCT
        source_schema AS SourceSchema,
        source_table AS TableName,
        OBJECT_ID(source_schema + '.' + source_table) AS ObjectId
    FROM
        sys.dm_exec_describe_first_result_set(@SQLQuery, NULL, 1) sp
    WHERE sp.error_number IS NULL AND NOT sp.source_table IS NULL

    SELECT
        t.TableName,
        migs.group_handle,
        migs.unique_compiles,
        migs.user_seeks,
        migs.user_scans,
        migs.last_user_seek,
        migs.last_user_scan,
        mid.statement,
        mid.equality_columns,
        mid.inequality_columns,
        mid.included_columns
    FROM
        @TableNames AS t
    INNER JOIN
        sys.dm_db_missing_index_groups mig ON mig.index_handle IN (
            SELECT index_handle
            FROM sys.dm_db_missing_index_details
            WHERE object_id = t.ObjectId
        )
    INNER JOIN
        sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
    INNER JOIN
        sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
    WHERE
        mid.database_id = DB_ID();
END;
