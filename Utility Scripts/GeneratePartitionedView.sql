/*
Author: Eitan Blumin | @EitanBlumin, https://www.eitanblumin.com
Create Date: 2016-06-03
Last Update: 2020-05-19
Description:
This procedure creates a partitioned view on top of identically-named tables that exist in multiple databases.

Parameters:

@DBNamePattern		- Database name pattern to use for filtering the relevant databases
@TableSchemaName	- The schema name of the tables to look for in each database
@TableName		- The table name to look for in each database.
@ViewName		- The name to give to the new partitioned view. If the view already exists, it will be overwritten.
@ColumnsList		- Comma-separated list of column names to use for the view. Columns missing in underlying tables will be replaced with NULL.

Examples:

EXEC GeneratePartitionedView 'SalesDB%', 'SalesLT', 'SalesOrderDetail', 'dbo.SalesOrderDetailView', N'[SalesOrderID], [SalesOrderDetailID], [OrderQty], [ProductID], [UnitPrice], [UnitPriceDiscount], [LineTotal], [ModifiedDate]'

EXEC GeneratePartitionedView 'SalesDB%', 'SalesLT', 'SalesOrderHeader', 'SalesOrderHeaderView', N'SalesOrderID,OrderDate,DueDate,ShipDate,Status,SalesOrderNumber,PurchaseOrderNumber,AccountNumber,CustomerID,ShipToAddressID,BillToAddressID,ShipMethod,SubTotal,TaxAmt,Freight,TotalDue,ModifiedDate'

*/
CREATE PROCEDURE [dbo].[GeneratePartitionedView]
	@DBNamePattern SYSNAME,
	@TableSchemaName SYSNAME = 'dbo',
	@TableName SYSNAME,
	@ViewName SYSNAME,
	@ColumnsList NVARCHAR(MAX),
	@Verbose BIT = 0
AS
SET NOCOUNT, XACT_ABORT, ARITHABORT ON;
DECLARE @ColumnValueSet NVARCHAR(MAX)
SET @ColumnsList = REPLACE(REPLACE(@ColumnsList, ']',''),'[','')

-- If DB Compatibility Level lower than 2016, use a values constructor:
IF (SELECT compatibility_level FROM sys.databases WHERE database_id = DB_ID()) < 130
	SET @ColumnValueSet = N'(VALUES(''' + REPLACE(REPLACE(@ColumnsList,' ',''), ',','''),(''') + N''')) AS Q([name])'

-- Otherwise, use STRING_SPLIT which would be more reliable
ELSE
	SET @ColumnValueSet = N'(SELECT RTRIM(LTRIM([value])) FROM STRING_SPLIT(@ColumnsList, '','') AS spl) AS Q([name])'

IF @Verbose = 1 PRINT @ColumnValueSet

DECLARE @CMD NVARCHAR(MAX), @TotalCMD NVARCHAR(MAX)
DECLARE @CurrPrefix NVARCHAR(1000), @CurrDB SYSNAME, @CurrColList NVARCHAR(MAX)

RAISERROR('Retrieving columns and databases...',0,1);

DECLARE DBs CURSOR FOR
SELECT prefix, name
FROM
(
	select QUOTENAME(name) as prefix, name, 1 AS priority, create_date
	from master.sys.databases
	where name like @DBNamePattern
) AS Q
ORDER BY priority ASC, create_date ASC

OPEN DBs
FETCH NEXT FROM DBs INTO @CurrPrefix, @CurrDB

WHILE @@FETCH_STATUS = 0
BEGIN
	RAISERROR(N'%s...',0,1,@CurrPrefix) WITH NOWAIT;
	SET @CurrColList = NULL
	SET @CMD = 'DECLARE @Columns AS TABLE(name SYSNAME, ID int IDENTITY(1,1) PRIMARY KEY);
	SET NOCOUNT ON;
	INSERT INTO @Columns
	SELECT name
	FROM ' + @ColumnValueSet + N';
	
	SELECT @ColList = ISNULL(@ColList + '', '', '''') + ISNULL(QUOTENAME(C.name), ''NULL AS '' + QUOTENAME(Q.name))
	FROM 
		' + @CurrPrefix + N'.sys.columns C with(nolock)
	INNER JOIN 
		' + @CurrPrefix + N'.sys.tables T with(nolock)
	ON
		C.object_id = T.object_id
	AND T.name = @TableName
	RIGHT JOIN @Columns AS Q
	ON
		C.name = Q.name
	'
	IF @Verbose = 1 PRINT @CMD

	EXEC sp_executesql @CMD, N'@TableName SYSNAME, @ColList NVARCHAR(MAX) OUTPUT, @ColumnsList NVARCHAR(MAX)', @TableName, @CurrColList OUTPUT, @ColumnsList;
	
	SET @TotalCMD = ISNULL(@TotalCMD + N'
	UNION ALL', '') + N'
	SELECT ' + @CurrColList + N'
	FROM ' + @CurrPrefix + N'.' + QUOTENAME(@TableSchemaName) + N'.' + QUOTENAME(@TableName)
	
	FETCH NEXT FROM DBs INTO @CurrPrefix, @CurrDB
END

CLOSE DBs
DEALLOCATE DBs

IF OBJECT_ID(@ViewName) IS NOT NULL
	SET @TotalCMD = N'ALTER VIEW ' + @ViewName + N'
AS ' + @TotalCMD
ELSE
	SET @TotalCMD = N'CREATE VIEW ' + @ViewName + N'
AS ' + @TotalCMD

RAISERROR('Creating %s...',0,1,@ViewName);
IF @Verbose = 1 PRINT @TotalCMD
EXECUTE(@TotalCMD);

IF @@ERROR = 0
	RAISERROR('View created successfully!',0,1);
ELSE
	RAISERROR('Error occured while creating view!',0,1);
	
GO

