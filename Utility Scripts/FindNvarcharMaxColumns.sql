/*==============================================================================================
===========================FIND REDUNDANT VARCHAR\NVARCHAR(MAX) DATATYPE USAGE==========================

Author: Eric Rouach, Madeira Data Solutions
Last Modified: 2023-07-28

Scope: Database

Description:
The following script will return all columns of VARCHAR(MAX) or
NVARCHAR(MAX) data type and check whether the MAX value is redundant or not.
In case it is fine to have it, then the remediation command will tell you to keep it "as is".
Otherwise, you will be advised to alter the column's capacity to a recommended value.

The result set also displays a remediation command!

!!!!!!!!!!!!!!!!!!!!!!!!!!!WARNING!!!!!!!!!!!!!!!!!!!!!!!!!!! 
DATATYPE MODIFICATION MIGHT TAKE YOUR APP DOWN!!!
DO NOT RUN THE REMEDIATION COMMAND BEFORE COORDINATING THE CHANGE WITH APP DEVELOPMENT TEAM!!!
==============================================================================================*/

SET NOCOUNT ON
SET ANSI_WARNINGS OFF

--create temp table to hold final result
IF OBJECT_ID('tempdb..#NvarcharMax') IS NOT NULL
DROP TABLE #NvarcharMax
CREATE TABLE #NvarcharMax
	(
		SchemaName NVARCHAR(255),
		TableName NVARCHAR(255),
		ColumnName NVARCHAR(255),
		MaxValueLen INT,
		RemediationCommand NVARCHAR(4000)
	)
;
--declare cursor variables
DECLARE @SchemaName NVARCHAR(255)
DECLARE @TableName NVARCHAR(255)
DECLARE @ColumnName NVARCHAR(255)
DECLARE @DataType NVARCHAR(255)

--cursor over all unique combinations of schema, table and column for NVARCHAR(MAX) columns
DECLARE ColumnsCursor CURSOR LOCAL FAST_FORWARD
FOR
	SELECT DISTINCT
	    TABLE_SCHEMA,
		TABLE_NAME,
		COLUMN_NAME,
		DATA_TYPE
	FROM 
		information_schema.columns isc
		INNER JOIN
		sys.objects o ON isc.TABLE_NAME = o.name
	WHERE 
		(isc.DATA_TYPE = 'varchar' OR isc.DATA_TYPE = 'nvarchar')
		AND
		isc.CHARACTER_MAXIMUM_LENGTH = -1 -- -1 refers to (MAX)
		AND
		o.type = 'U' -- tables only

OPEN ColumnsCursor 
FETCH NEXT FROM ColumnsCursor INTO @SchemaName, @TableName, @ColumnName, @DataType
WHILE @@FETCH_STATUS = 0  
BEGIN

	DECLARE @FullyQualifiedTblName NVARCHAR(255)
	SET @FullyQualifiedTblName = QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TableName)

	DECLARE @MaxValueLen INT

	DECLARE @GetMaxLenQuery NVARCHAR(255)
	SET @GetMaxLenQuery = 
	N'SELECT @MaxValueLen = MAX(LEN('+@ColumnName+')) FROM '+@FullyQualifiedTblName+' WITH(NOLOCK)'

	EXEC sp_executesql 
        @stmt = @GetMaxLenQuery
      , @Params = N'@MaxValueLen INT OUTPUT'
      , @MaxValueLen = @MaxValueLen OUTPUT;

	--preserve nullability value of column
	DECLARE @Nullability NVARCHAR(128)
	IF 
	(
		SELECT 
			IS_NULLABLE
		FROM 
			information_schema.columns 
		WHERE 
			TABLE_SCHEMA = @SchemaName
			AND
			TABLE_NAME = @TableName
			AND
			COLUMN_NAME = @ColumnName
	) 
	= 'NO'
	SET @Nullability = 'NOT NULL'
	ELSE
	SET @Nullability = 'NULL'

	DECLARE @TargetMaxValue INT
	DECLARE @RemediationCommand NVARCHAR(4000)

	IF @MaxValueLen >= 3200 OR @MaxValueLen IS NULL
		SET @RemediationCommand = N'Keep it as it is.'

	ELSE IF @MaxValueLen < 3200 AND @DataType = 'varchar'
	    begin
			SET @TargetMaxValue = CASE
								   WHEN @MaxValueLen <= 25 THEN 55
								   WHEN @MaxValueLen <= 255 THEN 255
								   WHEN @MaxValueLen <= 4000 THEN 4000
								  END
			SET @RemediationCommand = N'ALTER TABLE '+@FullyQualifiedTblName+' ALTER COLUMN '+QUOTENAME(@ColumnName)+' VARCHAR('+CAST(@TargetMaxValue AS NVARCHAR(4000))+') '+@Nullability+';'
	    end

	ELSE IF @MaxValueLen < 3200 AND @DataType = 'nvarchar'
		begin
			SET @TargetMaxValue = CASE
								   WHEN @MaxValueLen <= 25 THEN 55
								   WHEN @MaxValueLen <= 255 THEN 255
								   WHEN @MaxValueLen <= 4000 THEN 4000
								  END
			SET @RemediationCommand = N'ALTER TABLE '+@FullyQualifiedTblName+' ALTER COLUMN '+QUOTENAME(@ColumnName)+' NVARCHAR('+CAST(@TargetMaxValue AS NVARCHAR(4000))+') '+@Nullability+';'
		end

	INSERT INTO #NvarcharMax
	(SchemaName, TableName, ColumnName, MaxValueLen, RemediationCommand)
	VALUES
	(
	@SchemaName, 
	@TableName, 
	@ColumnName, 
	@MaxValueLen,
	@RemediationCommand
	)
FETCH NEXT FROM ColumnsCursor INTO @SchemaName, @TableName, @ColumnName, @Datatype
END          
CLOSE ColumnsCursor  
DEALLOCATE ColumnsCursor 

SELECT 
	SchemaName,
	TableName,
	ColumnName,
	MaxValueLen,
	RemediationCommand
FROM 
	#NvarcharMax
ORDER BY
	MaxValueLen DESC


--=========================Hope it helps!=========================