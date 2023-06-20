
DECLARE @Database sysname = 'DatabaseName'
DECLARE @SourceLinkedServer sysname = 'LinkedServerNameToSourceServer'
DECLARE @MinimumGapThreshold int = 50

DECLARE @CurrArticle sysname
DECLARE @ArticleFullName sysname, @CMD nvarchar(max)
DECLARE @LocalTotalRowCount int, @PKColumns nvarchar(4000)
DECLARE @RemoteTotalRowCount int, @RemotePKColumns nvarchar(4000)
DECLARE @spExecuteSqlLocal sysname, @spExecuteSqlRemote sysname
DECLARE @Diff int

DECLARE articles CURSOR
LOCAL FAST_FORWARD
FOR
select distinct article
from [distribution]..MSarticles
where publisher_db = @Database

OPEN articles

WHILE 1=1
BEGIN
	FETCH NEXT FROM articles INTO @CurrArticle;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @ArticleFullName = QUOTENAME(@Database) + '.' + @CurrArticle
	SET @spExecuteSqlLocal = QUOTENAME(@Database) + N'..sp_executesql'
	SET @spExecuteSqlRemote = QUOTENAME(@SourceLinkedServer) + N'.' + QUOTENAME(@Database) + N'..sp_executesql'

	SET @CMD = N'
SELECT @PKColumns = ISNULL(@PKColumns + N'','', N'''') + c.name
FROM ' + @Database + N'.sys.indexes AS ix
INNER JOIN ' + @Database + N'.sys.index_columns AS ic ON ix.object_id = ic.object_id AND ix.index_id = ic.index_id
INNER JOIN ' + @Database + N'.sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE ix.is_primary_key = 1
AND ix.object_id = OBJECT_ID(@ArticleFullName)

SELECT @totalRowCount = SUM(p.[rows])
FROM ' + @Database + N'.sys.partitions AS p
WHERE p.index_id <= 1
AND p.object_id = OBJECT_ID(@ArticleFullName)'

	EXEC @spExecuteSqlRemote @CMD
		, N'@ArticleFullName sysname, @PKColumns nvarchar(4000) OUTPUT, @totalRowCount int OUTPUT'
		, @ArticleFullName, @RemotePKColumns OUTPUT, @RemoteTotalRowCount OUTPUT
	
	EXEC @spExecuteSqlLocal @CMD
		, N'@ArticleFullName sysname, @PKColumns nvarchar(4000) OUTPUT, @totalRowCount int OUTPUT'
		, @ArticleFullName, @PKColumns OUTPUT, @LocalTotalRowCount OUTPUT

	IF ABS(@RemoteTotalRowCount - @LocalTotalRowCount) >= @MinimumGapThreshold
	BEGIN
		SET @Diff = @RemoteTotalRowCount - @LocalTotalRowCount
		RAISERROR(N'Rowcount discrepancy for %s (remote: %d, local: %d, diff: %d)',0,1,@ArticleFullName, @RemoteTotalRowCount, @LocalTotalRowCount, @Diff) WITH NOWAIT;
	END
END

CLOSE articles;
DEALLOCATE articles;
