/*
Based on Daniel Farina's script here:
https://www.mssqltips.com/sqlservertip/5699/auto-generate-create-table-script-based-on-sql-server-query/

This variant generates the table creation script by executing the sys.dm_exec_describe_first_result_set function.
*/
CREATE FUNCTION dbo.fn_Get_Table_Structure (@SourceTableName AS NVARCHAR(1000), @NewTableName AS NVARCHAR(1000) = NULL) 
RETURNS NVARCHAR(MAX)
AS
BEGIN

DECLARE @SQL AS NVARCHAR(MAX),  @InputSQL AS NVARCHAR(4000);
DECLARE @NewLine NVARCHAR(2) = CHAR(13) + CHAR(10) -- CRLF

SET @InputSQL = N'SELECT TOP 1 * FROM ' + @SourceTableName + N' WHERE 1=0';

SET @SQL = 'CREATE TABLE ' + ISNULL(@NewTableName, 'TableName') + ' ('

SELECT @SQL += @NewLine + QUOTENAME([name]) + ' ' + system_type_name
        + ISNULL('  COLLATE ' + collation_name COLLATE database_default + ' ', '')
        + CASE WHEN is_nullable = 0 THEN ' NOT NULL ' ELSE ' NULL ' END
        + ',' 
FROM
(
    SELECT TOP 100 PERCENT
            [name] ,
            is_nullable ,
            system_type_name ,
            collation_name
    FROM    sys.dm_exec_describe_first_result_set(@InputSQL, NULL, NULL)
    WHERE   is_hidden = 0
    ORDER BY column_ordinal ASC 
) AS q

SET @SQL = LEFT(@SQL, LEN(@SQL) - 1) + @NewLine + ')'

RETURN @SQL
END 
