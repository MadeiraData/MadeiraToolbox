/*
This procedure is designed to take a SQL query as input, specified through the @SQLQuery parameter, and dissect it to unveil the underlying schema and tables it interacts with.
Source: https://techcommunity.microsoft.com/t5/azure-database-support-blog/lesson-learned-481-query-performance-analysis-tips/ba-p/4088795
*/
CREATE PROCEDURE sp_AnalyzeQueryStatistics
    @SQLQuery NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @TableNames TABLE (
        SourceSchema NVARCHAR(128),
        TableName NVARCHAR(128)
    );

    INSERT INTO @TableNames (SourceSchema, TableName)
    SELECT DISTINCT
        source_schema AS SourceSchema,
        source_table AS TableName
    FROM
        sys.dm_exec_describe_first_result_set(@SQLQuery, NULL, 1) sp
		WHERE sp.error_number IS NULL AND NOT sp.source_table is NULL

    SELECT
        t.TableName,
        s.name AS StatisticName,
        STATS_DATE(s.object_id, s.stats_id) AS LastUpdated,
        sp.rows,
        sp.rows_sampled,
        sp.modification_counter
    FROM
        @TableNames AS t
    INNER JOIN
        sys.stats AS s ON s.object_id = OBJECT_ID(QUOTENAME(t.SourceSchema) + '.' + QUOTENAME(t.TableName))
    CROSS APPLY
        sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp;
END;
