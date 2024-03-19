/***********************************************************************************	
	
				Copyright: Eitan Blumin (c) 2018
	https://gist.github.com/EitanBlumin/79222fc2be5163cec828d0a69270a0ab

Description:
Generate Script to allow performing ONLINE index operations and heavy changes on huge tables, without needing Enterprise edition of SQL Server

***********************************************************************************/

-- TODO: Rename the _NEW object names to their original names (primary key, default and check constraints)
-- TODO: Identify constraints with NOCHECK
-- TODO: Identify user permissions on source table
-- TODO: Add incremental implementation to the DELTA table (using an indexed LastModifiedTime column + extended property to track last sync)

GO
IF OBJECT_ID('tempdb..#PrintMax', 'P') IS NOT NULL DROP PROCEDURE #PrintMax;
GO
-----------------------------------------------------------------------------------------------
-- This temporary procedure is a compact version of PrintMax originally written by Ben Dill
-- Copyright: https://weblogs.asp.net/bdill/sql-server-print-max
-----------------------------------------------------------------------------------------------
-- This procedure was created to properly print nvarchar(max) since the print statement can
-- only handle NVARCHAR(4000), we break the input down into 4000 byte blocks and print
-- upto the last linebreak before the 4000 byte cutoff
CREATE PROCEDURE #PrintMax @iInput NVARCHAR(MAX)
AS
BEGIN
 IF (@iInput IS NULL) RETURN;
 DECLARE @LineBreakIndex INT,@SearchLength INT = 4000;
 WHILE (LEN(@iInput) > @SearchLength) BEGIN
  SET @LineBreakIndex = CHARINDEX(CHAR(10) + CHAR(13), REVERSE(LEFT(@iInput, @SearchLength) COLLATE database_default));
  PRINT LEFT(@iInput, @SearchLength - @LineBreakIndex + 1);
  SET @iInput = RIGHT(@iInput COLLATE database_default, LEN(@iInput) - @SearchLength + @LineBreakIndex - 1);
 END;
 IF (LEN(@iInput) > 0) PRINT @iInput;
END
GO
DECLARE
/*********************************************************************************************
									PARAMETERS
**********************  !!! DO NOT EDIT ANYTHING ABOVE THIS LINE !!!  ************************/

	 @SourceTableName					SYSNAME		= 'dbo.OnlineIndexTest'
	,@ChunkIntervalForSingleColumnPK	INT			= 1000
	,@OperationDeltaColumn				SYSNAME		= '___Operation'	-- must be different from any existing table columns
	,@PrecedenceDeltaColumn				SYSNAME		= '___Precedence'	-- must be different from any existing table columns
	,@RowRankDeltaColumn				SYSNAME		= '___RowRank'		-- must be different from any existing table columns
	,@DeltaTriggerNamePrefix			SYSNAME		= '___TR_DELTA_'	-- the name of the source table will be added to the trigger name prefix
	,@CopyUsingNoLock					BIT			= 0
	,@NewTableNamePostfix				SYSNAME		= '___NEW'
	,@DeltaTableNamePostfix				SYSNAME		= '___DELTA'
	,@OldTableNamePostfix				SYSNAME		= '___OLD'
	,@CustomPKReplacementIndex			SYSNAME		= NULL -- If you specify a non-null value, then this index will replace the PK on the new table (clustered)

/*********************************************************************************************
									/PARAMETERS
**********************  !!! DO NOT EDIT ANYTHING BELOW THIS LINE !!!  ************************/


DECLARE @SourceTableID INT = OBJECT_ID(@SourceTableName), @CleanSourceTableName SYSNAME = OBJECT_NAME(OBJECT_ID(@SourceTableName));
DECLARE @DeltaTriggerName SYSNAME = @DeltaTriggerNamePrefix + @CleanSourceTableName;

-- Some validations:
IF @SourceTableID IS NULL
BEGIN
	RAISERROR(N'VALIDATION ERROR: Source Table %s not found!',16,1,@SourceTableName);
	GOTO Quit;
END
ELSE
	SET @SourceTableName = QUOTENAME(OBJECT_SCHEMA_NAME(@SourceTableID)) + N'.' + QUOTENAME(OBJECT_NAME(@SourceTableID))

IF EXISTS (SELECT * FROM sys.sysdepends WHERE deptype = 1 AND id <> depid AND depid = @SourceTableID)
BEGIN
	RAISERROR(N'VALIDATION ERROR: Table %s is invalid for this operation because it''s referenced by schema-bound object(s)!', 16,1, @SourceTableName);
	GOTO Quit;
END

IF EXISTS (SELECT * FROM sys.foreign_keys WHERE referenced_object_id = @SourceTableID)
BEGIN
	RAISERROR(N'VALIDATION ERROR: Table %s is invalid for this operation because it''s referenced by Foreign Key(s)!', 16,1, @SourceTableName);
	GOTO Quit;
END

IF @CustomPKReplacementIndex IS NOT NULL AND NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = @SourceTableID AND name = @CustomPKReplacementIndex)
BEGIN
	RAISERROR(N'VALIDATION ERROR: Index %s not found on source table!', 16,1, @CustomPKReplacementIndex);
	GOTO Quit;
END

IF EXISTS (SELECT * FROM sys.triggers WHERE name = @DeltaTriggerName AND OBJECT_SCHEMA_NAME(object_id) = OBJECT_SCHEMA_NAME(@SourceTableID) AND parent_id <> @SourceTableID)
BEGIN
	RAISERROR(N'VALIDATION ERROR: Trigger %s already exists on a different table!', 16,1, @DeltaTriggerName);
	GOTO Quit;
END

-- Local variables:
DECLARE
	@NewTableName SYSNAME = QUOTENAME(OBJECT_SCHEMA_NAME(@SourceTableID)) + N'.' + QUOTENAME(@CleanSourceTableName + @NewTableNamePostfix),
	@DeltaTableName SYSNAME = QUOTENAME(OBJECT_SCHEMA_NAME(@SourceTableID)) + N'.' + QUOTENAME(@CleanSourceTableName + @DeltaTableNamePostfix),
	@OldTableName SYSNAME = QUOTENAME(OBJECT_SCHEMA_NAME(@SourceTableID)) + N'.' + QUOTENAME(@CleanSourceTableName + @OldTableNamePostfix)

DECLARE
	@CMD NVARCHAR(MAX),
	@IsPKValid BIT,
	@PKindex NVARCHAR(MAX),
	@PKcolumnDefinitions NVARCHAR(MAX),
	@PKcolumnList NVARCHAR(MAX),
	@PKjoin NVARCHAR(MAX),
	@PKcolumnCount INT,
	@RenameCommands NVARCHAR(MAX),
	@vbCrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10)

-- A few more validations:
IF OBJECT_ID(@OldTableName) IS NOT NULL
BEGIN
	RAISERROR(N'VALIDATION ERROR: OLD table %s already exists!', 16, 1, @OldTableName);
	GOTO Quit;
END
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(@SourceTableName) AND name IN (@OperationDeltaColumn, @PrecedenceDeltaColumn, @RowRankDeltaColumn))
BEGIN
	RAISERROR(N'VALIDATION ERROR: One or more of the following columns already exists in %s: @OperationDeltaColumn, @PrecedenceDeltaColumn, @RowRankDeltaColumn', 16, 1, @SourceTableName);
	GOTO Quit;
END

-- Build definition of PK without the name (will be used for both NEW and DELTA tables)
SELECT
	@CustomPKReplacementIndex = pk.name,
	@IsPKValid = pk.is_primary_key,
	@PKcolumnList = STUFF(
		(SELECT N', ' + QUOTENAME(c.name) COLLATE database_default FROM sys.index_columns AS ic INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id WHERE ic.is_included_column = 0 AND ic.object_id = pk.object_id AND ic.index_id = pk.index_id ORDER BY ic.key_ordinal ASC FOR XML PATH(''))
		, 1,2,N''),
	@PKindex = N' PRIMARY KEY CLUSTERED ('
	+ STUFF(
		(SELECT N', ' + QUOTENAME(c.name) COLLATE database_default + N' ' + CASE is_descending_key WHEN 1 THEN N'DESC' ELSE N'ASC' END FROM sys.index_columns AS ic INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id WHERE ic.is_included_column = 0 AND ic.object_id = pk.object_id AND ic.index_id = pk.index_id ORDER BY ic.key_ordinal ASC FOR XML PATH(''))
		, 1,2,N'')
	+ N') 
	WITH (FILLFACTOR = ' + CONVERT(nvarchar(max), ISNULL(NULLIF(pk.fill_factor,0),100))
			+ N', ALLOW_ROW_LOCKS = '	+ CASE pk.allow_row_locks	WHEN 1 THEN N'ON' ELSE N'OFF' END
			+ N', ALLOW_PAGE_LOCKS = '	+ CASE pk.allow_page_locks	WHEN 1 THEN N'ON' ELSE N'OFF' END
			+ N', IGNORE_DUP_KEY = '	+ CASE pk.ignore_dup_key	WHEN 1 THEN N'ON' ELSE N'OFF' END
			+ N', PAD_INDEX = '			+ CASE pk.is_padded			WHEN 1 THEN N'ON' ELSE N'OFF' END
	+ N') 
	ON ' + QUOTENAME(ds.name) COLLATE database_default
FROM sys.indexes AS pk
INNER JOIN sys.data_spaces AS ds ON pk.data_space_id = ds.data_space_id 
WHERE object_id = @SourceTableID
AND (
	(
	@CustomPKReplacementIndex IS NULL 
	AND pk.is_primary_key = 1
	AND pk.index_id = 1 -- clustered
	)
	OR
	(
	@CustomPKReplacementIndex IS NOT NULL 
	AND pk.name = @CustomPKReplacementIndex
	)
	)

IF @PKindex IS NULL
BEGIN
	RAISERROR(N'VALIDATION ERROR: Table %s is invalid for this operation because it does not have a Clustered Primary Key! Either create a clustered PK for the table, or specify a @CustomPKReplacementIndex.', 16, 1, @SourceTableName);
	GOTO Quit;
END

-- If a Custom PK index was specified, make sure it really is unique
IF @IsPKValid = 0
BEGIN
	SET @CMD = N'
	SELECT @IsPKValid = 1
	WHERE NOT EXISTS
	(SELECT ' + @PKcolumnList + N' FROM ' + @SourceTableName + N' WITH (NOLOCK) GROUP BY ' + @PKcolumnList + N' HAVING COUNT(*) > 1)'

	EXEC sp_executesql @CMD, N'@IsPKValid BIT OUTPUT', @IsPKValid OUTPUT;
	
	IF @IsPKValid = 0
	BEGIN
		RAISERROR(N'VALIDATION ERROR: Custom Index %s cannot be used as a replacement Primary Key, because it contains duplicate values!', 16, 1, @CustomPKReplacementIndex);
		GOTO Quit;
	END
END

-- 1. Create empty NEW table

RAISERROR(N'
/*************************************************************************
**************************************************************************

					!!! BEGINNING PART 1 !!!

	This section can be executed freely without affecting any live objects.

			!!! CAREFUL NOT TO RUN PART 2 IMMEDIATELY !!!

**************************************************************************
**************************************************************************/


/*****************  Creating NEW table %s ***************************/
/*
This section was partly adapted from the sp_GetDDL script by Lowell Izaguirre: http://www.sqlservercentral.com/scripts/SQL+Server+2005/67515/
*/

IF OBJECT_ID(N''%s'') IS NULL
BEGIN',0,1,@NewTableName,@NewTableName) WITH NOWAIT;

SET @CMD = N'
	CREATE TABLE ' + @NewTableName + N' ( '
  SELECT
    @CMD = @CMD
    + CASE
        WHEN [COLS].[is_computed] = 1
        THEN @vbCrLf
             + QUOTENAME([COLS].[name])
             + ' '
             + 'AS ' + ISNULL([CALC].[definition],'')
             + CASE 
                 WHEN [CALC].[is_persisted] = 1 
                 THEN ' PERSISTED'
                 ELSE ''
               END
        ELSE @vbCrLf
             + QUOTENAME([COLS].[name])
             + ' '
             + UPPER(TYPE_NAME([COLS].[user_type_id]))
             + CASE
-- data types with precision and scale  IE DECIMAL(18,3), NUMERIC(10,2)
               WHEN TYPE_NAME([COLS].[user_type_id]) IN ('decimal','numeric')
               THEN '('
                    + CONVERT(VARCHAR,[COLS].[precision])
                    + ','
                    + CONVERT(VARCHAR,[COLS].[scale])
                    + ') '
                    + SPACE(6 - LEN(CONVERT(VARCHAR,[COLS].[precision])
                    + ','
                    + CONVERT(VARCHAR,[COLS].[scale])))
                    + SPACE(7)
                    + CASE
                        WHEN COLUMNPROPERTY ( @SourceTableID , [COLS].[name] , 'IsIdentity' ) = 0
                        THEN ''
                        ELSE ' IDENTITY('
                               + CONVERT(VARCHAR,ISNULL(IDENT_SEED(@SourceTableName),1) )
                               + ','
                               + CONVERT(VARCHAR,ISNULL(IDENT_INCR(@SourceTableName),1) )
                               + ')'
                        END
                    + CASE  WHEN [COLS].[is_sparse] = 1 THEN ' sparse' ELSE '       ' END
                    + CASE
                        WHEN [COLS].[is_nullable] = 0
                        THEN ' NOT NULL'
                        ELSE '     NULL'
                      END
-- data types with scale  IE datetime2(7),TIME(7)
               WHEN TYPE_NAME([COLS].[user_type_id]) IN ('datetime2','datetimeoffset','time')
               THEN CASE 
                      WHEN [COLS].[scale] < 7 THEN
                      '('
                      + CONVERT(VARCHAR,[COLS].[scale])
                      + ') '
                    ELSE 
                      '    '
                    END
                    + SPACE(4)
                    + '        '
                    + CASE  WHEN [COLS].[is_sparse] = 1 THEN ' sparse' ELSE '       ' END
                    + CASE
                        WHEN [COLS].[is_nullable] = 0
                        THEN ' NOT NULL'
                        ELSE '     NULL'
                      END

--data types with no/precision/scale,IE  FLOAT
               WHEN  TYPE_NAME([COLS].[user_type_id]) IN ('float') --,'real')
               THEN
               --addition: if 53, no need to specifically say (53), otherwise display it
                    CASE
                      WHEN [COLS].[precision] = 53
                      THEN SPACE(11 - LEN(CONVERT(VARCHAR,[COLS].[precision])))
                           + SPACE(7)
                           + CASE  WHEN [COLS].[is_sparse] = 1 THEN ' sparse' ELSE '       ' END
                           + CASE
                               WHEN [COLS].[is_nullable] = 0
                               THEN ' NOT NULL'
                               ELSE '     NULL'
                             END
                      ELSE '('
                           + CONVERT(VARCHAR,[COLS].[precision])
                           + ') '
                           + SPACE(6 - LEN(CONVERT(VARCHAR,[COLS].[precision])))
                           + SPACE(7)
                           + CASE  WHEN [COLS].[is_sparse] = 1 THEN ' sparse' ELSE '       ' END
                           + CASE
                               WHEN [COLS].[is_nullable] = 0
                               THEN ' NOT NULL'
                               ELSE '     NULL'
                             END
                      END
               WHEN  TYPE_NAME([COLS].[user_type_id]) IN ('char','varchar','binary','varbinary')
               THEN CASE
                      WHEN  [COLS].[max_length] = -1
                      THEN  '(max)'
                            + SPACE(6 - LEN(CONVERT(VARCHAR,[COLS].[max_length])))
                            + SPACE(7)
                            ----collate to comment out when not desired
                            --+ CASE
                            --    WHEN COLS.collation_name IS NULL
                            --    THEN ''
                            --    ELSE ' COLLATE ' + COLS.collation_name
                            --  END
                            + CASE  WHEN [COLS].[is_sparse] = 1 THEN ' sparse' ELSE '       ' END
                            + CASE
                                WHEN [COLS].[is_nullable] = 0
                                THEN ' NOT NULL'
                                ELSE '     NULL'
                              END
                      ELSE '('
                           + CONVERT(VARCHAR,[COLS].[max_length])
                           + ') '
                           + SPACE(6 - LEN(CONVERT(VARCHAR,[COLS].[max_length])))
                           + SPACE(7)
                           ----collate to comment out when not desired
                           --+ CASE
                           --     WHEN COLS.collation_name IS NULL
                           --     THEN ''
                           --     ELSE ' COLLATE ' + COLS.collation_name
                           --   END
                           + CASE  WHEN [COLS].[is_sparse] = 1 THEN ' sparse' ELSE '       ' END
                           + CASE
                               WHEN [COLS].[is_nullable] = 0
                               THEN ' NOT NULL'
                               ELSE '     NULL'
                             END
                    END
--data type with max_length ( BUT DOUBLED) ie NCHAR(33), NVARCHAR(40)
               WHEN TYPE_NAME([COLS].[user_type_id]) IN ('nchar','nvarchar')
               THEN CASE
                      WHEN  [COLS].[max_length] = -1
                      THEN '(max)'
                           + SPACE(5 - LEN(CONVERT(VARCHAR,([COLS].[max_length] / 2))))
                           + SPACE(7)
                           ----collate to comment out when not desired
                           --+ CASE
                           --     WHEN COLS.collation_name IS NULL
                           --     THEN ''
                           --     ELSE ' COLLATE ' + COLS.collation_name
                           --   END
                           + CASE  WHEN [COLS].[is_sparse] = 1 THEN ' sparse' ELSE '       ' END
                           + CASE
                               WHEN [COLS].[is_nullable] = 0
                               THEN  ' NOT NULL'
                               ELSE '     NULL'
                             END
                      ELSE '('
                           + CONVERT(VARCHAR,([COLS].[max_length] / 2))
                           + ') '
                           + SPACE(6 - LEN(CONVERT(VARCHAR,([COLS].[max_length] / 2))))
                           + SPACE(7)
                           ----collate to comment out when not desired
                           --+ CASE
                           --     WHEN COLS.collation_name IS NULL
                           --     THEN ''
                           --     ELSE ' COLLATE ' + COLS.collation_name
                           --   END
                           + CASE  WHEN [COLS].[is_sparse] = 1 THEN ' sparse' ELSE '       ' END
                           + CASE
                               WHEN [COLS].[is_nullable] = 0
                               THEN ' NOT NULL'
                               ELSE '     NULL'
                             END
                    END

               WHEN TYPE_NAME([COLS].[user_type_id]) IN ('datetime','money','text','image','real')
               THEN SPACE(18 - LEN(TYPE_NAME([COLS].[user_type_id])))
                    + '              '
                    + CASE  WHEN [COLS].[is_sparse] = 1 THEN ' sparse' ELSE '       ' END
                    + CASE
                        WHEN [COLS].[is_nullable] = 0
                        THEN ' NOT NULL'
                        ELSE '     NULL'
                      END

--  other data type 	IE INT, DATETIME, MONEY, CUSTOM DATA TYPE,...
               ELSE 
                             CASE
                                WHEN COLUMNPROPERTY ( @SourceTableID , [COLS].[name] , 'IsIdentity' ) = 0
                                THEN '              '
                                ELSE ' IDENTITY('
                                     + CONVERT(VARCHAR,ISNULL(IDENT_SEED(@SourceTableName),1) )
                                     + ','
                                     + CONVERT(VARCHAR,ISNULL(IDENT_INCR(@SourceTableName),1) )
                                     + ')'
                              END
                            + SPACE(2)
                            + CASE  WHEN [COLS].[is_sparse] = 1 THEN ' sparse' ELSE '       ' END
                            + CASE
                                WHEN [COLS].[is_nullable] = 0
                                THEN ' NOT NULL'
                                ELSE '     NULL'
                              END
               END
      END --iscomputed
    + ','
    FROM [sys].[columns] [COLS]
      LEFT OUTER JOIN [sys].[computed_columns] [CALC]
         ON  [COLS].[object_id] = [CALC].[object_id]
         AND [COLS].[column_id] = [CALC].[column_id]
    WHERE [COLS].[object_id]=@SourceTableID
    ORDER BY [COLS].[column_id];
	
	
  SET @CMD = SUBSTRING(@CMD,1,LEN(@CMD) -1) ;
  SET @CMD = @CMD + ')' COLLATE database_default + @vbCrLf ;

	-- Create PK on NEW table

	SELECT @CMD = @CMD + N'
	ALTER TABLE ' + @NewTableName + N' ADD CONSTRAINT ' + QUOTENAME(pk.name + @NewTableNamePostfix) COLLATE database_default
	,
	@RenameCommands = @RenameCommands + N'
EXEC sp_rename N''' +  pk.name COLLATE database_default + @NewTableNamePostfix + N''', N''' + pk.name COLLATE database_default + N''';'

	FROM sys.indexes AS pk
	INNER JOIN sys.data_spaces AS ds ON pk.data_space_id = ds.data_space_id 
	WHERE object_id = @SourceTableID
	AND pk.name = @CustomPKReplacementIndex

	PRINT @CMD + @PKindex

	-- Create default constraints on NEW table
	SET @CMD = N'';

	SELECT @CMD = @CMD + N'

	ALTER TABLE ' + @NewTableName + N' ADD CONSTRAINT ' + QUOTENAME(df.name + @NewTableNamePostfix) COLLATE database_default + N' DEFAULT ' + df.definition COLLATE database_default + N' FOR ' + QUOTENAME(c.name) COLLATE database_default + N';'
	,
	@RenameCommands = @RenameCommands + N'
EXEC sp_rename N''' +  df.name COLLATE database_default + @NewTableNamePostfix + N''', N''' + df.name COLLATE database_default + N''';'
	FROM sys.default_constraints AS df
	INNER JOIN sys.columns AS c
	ON df.parent_object_id = c.object_id
	AND df.parent_column_id = c.column_id
	WHERE parent_object_id = @SourceTableID

	EXEC #PrintMax @CMD;
	
RAISERROR(N'
END
ELSE
	RAISERROR(N''NEW table already exists!'',0,1) WITH NOWAIT;',0,1,@NewTableName) WITH NOWAIT;


-- 2. Parse PK columns (for join and update expressions and various column lists)
SELECT
	 @PKcolumnDefinitions = ISNULL(@PKcolumnDefinitions + N', ', N'') + QUOTENAME(c.name) COLLATE database_default + N' ' + t.name + N' ' + CASE c.is_nullable WHEN 1 THEN 'NULL' ELSE 'NOT NULL' END
	,@PKjoin = ISNULL(@PKjoin + N'
	AND ', N'') + N'Trgt.' + QUOTENAME(c.name) COLLATE database_default + N' = Src.' + QUOTENAME(c.name) COLLATE database_default
	,@PKcolumnCount = ISNULL(@PKcolumnCount,0) + 1
FROM sys.indexes AS pk
INNER JOIN sys.index_columns AS ic 
ON ic.is_included_column = 0 
AND ic.object_id = pk.object_id 
AND ic.index_id = pk.index_id 
INNER JOIN sys.columns AS c 
ON ic.object_id = c.object_id 
AND ic.column_id = c.column_id
INNER JOIN sys.types AS t
ON c.system_type_id = t.system_type_id
AND c.user_type_id = t.user_type_id 
WHERE pk.object_id = @SourceTableID
AND pk.name = @CustomPKReplacementIndex
ORDER BY ic.key_ordinal ASC 


-- 3. Create empty table DELTA

RAISERROR(N'
/******************  Creating DELTA table %s ***************************/

IF OBJECT_ID(N''%s'') IS NULL
BEGIN',0,1,@DeltaTableName,@DeltaTableName) WITH NOWAIT;

	SET @CMD = N'
	CREATE TABLE ' + @DeltaTableName + N'
	(
		' + @PKcolumnDefinitions + N',
		' + QUOTENAME(@OperationDeltaColumn) + N' CHAR(1) NOT NULL,
		' + @PKindex + N'
	);'

	EXEC #PrintMax @CMD
RAISERROR(N'
END
ELSE
	RAISERROR(N''DELTA table already exists!'',0,1) WITH NOWAIT;
	
	
/*************************************************************************
**************************************************************************

					!!! END OF PART 1 !!!

!!!	Any changes to the NEW table should be done NOW before continuing !!!

	This includes any index restructure, creation, or whatever changes
	that need to be done while the table is still empty

**************************************************************************
**************************************************************************/


/*************************************************************************
**************************************************************************

					!!! BEGINNING PART 2 !!!

	Once this section starts running, delta data will begin accumulating
	and initial data will be copied into the NEW table.
	This section is expected to run for a very long time, but the tables
	remain online and accessible.


						!!! WARNING !!!
	
	If there were any column changes in the NEW table (such as new columns,
	dropped or renamed columns), then you would need to make changes
	accordingly in the following scripts!

**************************************************************************
**************************************************************************/',0,1) WITH NOWAIT;

-- 4. Create AFTER trigger on source table
SET @CMD = N'IF EXISTS (SELECT * FROM sys.triggers WHERE name = N''' + @DeltaTriggerName + N''' AND QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) = N''' + QUOTENAME(OBJECT_SCHEMA_NAME(@SourceTableID)) + N''')
	DROP TRIGGER ' + QUOTENAME(OBJECT_SCHEMA_NAME(@SourceTableID)) + N'.' + QUOTENAME(@DeltaTriggerName) + N';
GO
/*********************************************************************
			Trigger for copying DELTA during runtime
*********************************************************************/
CREATE TRIGGER ' + QUOTENAME(OBJECT_SCHEMA_NAME(@SourceTableID)) + N'.' + QUOTENAME(@DeltaTriggerName) + N' ON ' + @SourceTableName + N'
AFTER INSERT,DELETE,UPDATE
AS
	SET NOCOUNT ON;

	; WITH Src AS
	(
		SELECT *, ' + QUOTENAME(@RowRankDeltaColumn) + N' = ROW_NUMBER() OVER (PARTITION BY ' + @PKcolumnList + N' ORDER BY ' + QUOTENAME(@PrecedenceDeltaColumn) + N' ASC)
		FROM
		(
			SELECT ' + @PKcolumnList + N', ''D'' AS ' + QUOTENAME(@OperationDeltaColumn) + N', 1 AS ' + QUOTENAME(@PrecedenceDeltaColumn) + N'
			FROM deleted
			UNION ALL
			SELECT ' + @PKcolumnList + N', ''I'' AS ' + QUOTENAME(@OperationDeltaColumn) + N', 0 AS ' + QUOTENAME(@PrecedenceDeltaColumn) + N'
			FROM inserted
		) AS d
	)
	MERGE INTO ' + @DeltaTableName + N' AS Trgt
	USING 
	(
		SELECT *
		FROM Src
		WHERE ' + QUOTENAME(@RowRankDeltaColumn) + N' = 1 -- In case of update, give precedence to the inserted row
	) AS Src
	ON
		' + @PKjoin + N'
	WHEN MATCHED AND Trgt.' + QUOTENAME(@OperationDeltaColumn) + N' <> Src.' + QUOTENAME(@OperationDeltaColumn) + N' THEN
		UPDATE SET ' + QUOTENAME(@OperationDeltaColumn) + N' = Src.' + QUOTENAME(@OperationDeltaColumn) + N'
	WHEN NOT MATCHED BY TARGET THEN
		INSERT (' + @PKcolumnList + N', ' + QUOTENAME(@OperationDeltaColumn) + N')
		VALUES (' + @PKcolumnList + N', ' + QUOTENAME(@OperationDeltaColumn) + N')
	;'

PRINT 'GO'

EXEC #PrintMax @CMD

PRINT 'GO'


-- 5. Initial migration from source table to NEW table, by chunks
DECLARE @PKtype NVARCHAR(MAX), @PKisIdentity BIT, @AllColumnsList NVARCHAR(MAX), @AllColumnsUpdateSet NVARCHAR(MAX)

SELECT @AllColumnsList = ISNULL(@AllColumnsList + N', ', N'') + QUOTENAME(c.name) COLLATE database_default
, @AllColumnsUpdateSet  = CASE WHEN ispk.cnt = 0 THEN ISNULL(@AllColumnsUpdateSet  + N',
	', N'') + QUOTENAME(c.name) COLLATE database_default + N' = Src.' + QUOTENAME(c.name) COLLATE database_default
	ELSE @AllColumnsUpdateSet END
FROM sys.columns AS c
OUTER APPLY
(
	SELECT cnt = COUNT(*) FROM sys.indexes AS pk INNER JOIN sys.index_columns AS ic ON pk.object_id = ic.object_id AND pk.index_id = ic.index_id
	WHERE pk.object_id = c.object_id AND ic.column_id = c.column_id
	AND pk.name = @CustomPKReplacementIndex
) AS ispk
WHERE c.object_id = @SourceTableID
AND c.is_computed = 0

-- If only 1 column in PK
IF @PKcolumnCount = 1
BEGIN
	
	SELECT
		 @PKtype = t.name
		 ,@PKisIdentity = c.is_identity
	FROM sys.indexes AS pk
	INNER JOIN sys.index_columns AS ic 
	ON ic.is_included_column = 0 
	AND ic.object_id = pk.object_id 
	AND ic.index_id = pk.index_id 
	INNER JOIN sys.columns AS c 
	ON ic.object_id = c.object_id 
	AND ic.column_id = c.column_id
	INNER JOIN sys.types AS t
	ON c.system_type_id = t.system_type_id
	AND c.user_type_id = t.user_type_id 
	WHERE pk.object_id = @SourceTableID
	AND pk.name = @CustomPKReplacementIndex

	SET @CMD = N'
/****************** Beginning Initial Copy into NEW Table ************************/

SET NOCOUNT ON;
DECLARE @TotalCount bigint, @CurrentCount bigint, @Percent varchar(10), @ChunkStart ' + @PKtype + N', @ChunkEnd ' + @PKtype + N', @ChunkFinish ' + @PKtype + N', @ChunkInterval ' + @PKtype + N' = ' + CONVERT(nvarchar(max), @ChunkIntervalForSingleColumnPK) + N'

SELECT @ChunkStart = MIN(' + @PKcolumnList + N'), @ChunkFinish = MAX(' + @PKcolumnList + N'), @TotalCount = COUNT_BIG(*), @CurrentCount = 0
FROM ' + @SourceTableName + CASE WHEN @CopyUsingNoLock = 1 THEN N' WITH(NOLOCK)' ELSE N'' END + N'

RAISERROR(N''Starting to copy data into ' + @NewTableName + N': %d to %d (total %I64d rows).'',0,1,@ChunkStart, @ChunkFinish, @TotalCount) WITH NOWAIT;

SET @ChunkEnd = @ChunkStart + @ChunkInterval
' + CASE WHEN @PKisIdentity = 1 THEN N'
SET IDENTITY_INSERT ' + @NewTableName + N' ON;' ELSE N'' END + N'

WHILE @ChunkStart <= @ChunkFinish
BEGIN
	INSERT INTO ' + @NewTableName + CASE WHEN @PKisIdentity = 1 THEN N'
	(' + @AllColumnsList + N')' ELSE N'' END + N'
	SELECT
	 ' + CASE WHEN @PKisIdentity = 1 THEN @AllColumnsList ELSE N'*' END + N'
	FROM ' + @SourceTableName + CASE WHEN @CopyUsingNoLock = 1 THEN N' WITH(NOLOCK)' ELSE N'' END + N'
	WHERE ' + @PKcolumnList + N' >= @ChunkStart
	AND ' + @PKcolumnList + N' <= @ChunkEnd
	AND ' + @PKcolumnList + N' <= @ChunkFinish
	
	SET @CurrentCount = @CurrentCount + @@ROWCOUNT
	SET @Percent = CONVERT(varchar, CONVERT(money, CONVERT(float,@CurrentCount) / CONVERT(float,@TotalCount) * 100.0)) + ''%''

	RAISERROR(N''%s	- %I64d / %I64d'', 0,1, @Percent, @CurrentCount, @TotalCount);
	
	SELECT @ChunkStart = MIN(' + @PKcolumnList + N'), @ChunkEnd = MAX(' + @PKcolumnList + N')
	FROM
	(
	SELECT TOP (@ChunkInterval) ' + @PKcolumnList + N'
	FROM ' + @SourceTableName + CASE WHEN @CopyUsingNoLock = 1 THEN N' WITH(NOLOCK)' ELSE N'' END + N'
	WHERE ' + @PKcolumnList + N' > @ChunkEnd
	ORDER BY ' + @PKcolumnList + N' ASC
	) AS a

	IF @@ROWCOUNT = 0
		SET @ChunkStart = @ChunkFinish + 1;

END

' + CASE WHEN @PKisIdentity = 1 THEN N'SET IDENTITY_INSERT ' + @NewTableName + N' OFF;' ELSE N'' END + N'

GO'
END
-- If more than 1 column in PK
ELSE
BEGIN
	DECLARE
		@PK2columnDefinitions NVARCHAR(MAX),
		@PK2columnList NVARCHAR(MAX),
		@PK2variableDefinitions NVARCHAR(MAX),
		@PK2variableList NVARCHAR(MAX),
		@PK2variableJoin NVARCHAR(MAX)

	SELECT
		 @PK2columnDefinitions = ISNULL(@PK2columnDefinitions + N', ', N'') + QUOTENAME(c.name) COLLATE database_default + N' ' + t.name + N' ' + CASE c.is_nullable WHEN 1 THEN 'NULL' ELSE 'NOT NULL' END
		,@PK2variableDefinitions = ISNULL(@PK2variableDefinitions + N', ', N'') + N'@pk' + CONVERT(nvarchar(max), ic.key_ordinal) +N' ' + t.name
		,@PK2columnList = ISNULL(@PK2columnList + N', ', N'') + QUOTENAME(c.name) COLLATE database_default
		,@PK2variableList = ISNULL(@PK2variableList + N', ', N'') + N'@pk' + CONVERT(nvarchar(max), ic.key_ordinal)
		,@PK2variableJoin = ISNULL(@PK2variableJoin + N'
		AND ', N'') + QUOTENAME(c.name) COLLATE database_default + N' = @pk' + CONVERT(nvarchar(max), ic.key_ordinal)
	FROM sys.indexes AS pk
	INNER JOIN sys.index_columns AS ic 
	ON ic.is_included_column = 0 
	AND ic.object_id = pk.object_id 
	AND ic.index_id = pk.index_id 
	INNER JOIN sys.columns AS c 
	ON ic.object_id = c.object_id 
	AND ic.column_id = c.column_id
	INNER JOIN sys.types AS t
	ON c.system_type_id = t.system_type_id
	AND c.user_type_id = t.user_type_id 
	WHERE pk.object_id = @SourceTableID
	AND pk.name = @CustomPKReplacementIndex
	AND ic.key_ordinal < (SELECT MAX(key_ordinal) FROM sys.index_columns AS ic2 WHERE ic2.is_included_column = 0 AND ic2.object_id = pk.object_id AND ic2.index_id = pk.index_id)
	ORDER BY ic.key_ordinal ASC 

	SET @CMD = N'
/****************** Beginning Initial Copy into NEW Table ************************/

SET NOCOUNT ON;
DECLARE @Chunks AS TABLE (' + @PK2columnDefinitions + N');

INSERT INTO @Chunks
SELECT ' + @PK2columnList + N'
FROM ' + @SourceTableName + CASE WHEN @CopyUsingNoLock = 1 THEN N' WITH(NOLOCK)' ELSE N'' END + N'
GROUP BY ' + @PK2columnList + N'

RAISERROR(N''Starting to copy data into ' + @NewTableName + N'. %d permutations in total.'',0,1,@@ROWCOUNT) WITH NOWAIT;

' + CASE WHEN @PKisIdentity = 1 THEN N'SET IDENTITY_INSERT ' + @NewTableName + N' ON;' ELSE N'' END + N'

DECLARE ' + @PK2variableDefinitions + N'

DECLARE Chunks CURSOR LOCAL FAST_FORWARD FOR
SELECT ' + @PK2columnList + N' FROM @Chunks

OPEN Chunks
FETCH NEXT FROM Chunks INTO ' + @PK2variableList + N'

WHILE @@FETCH_STATUS = 0
BEGIN
	INSERT INTO ' + @NewTableName + N'
	SELECT
	 *
	FROM ' + @SourceTableName + CASE WHEN @CopyUsingNoLock = 1 THEN N' WITH(NOLOCK)' ELSE N'' END + N'
	WHERE ' + @PK2variableJoin + N'

	FETCH NEXT FROM Chunks INTO ' + @PK2variableList + N'
END

CLOSE Chunks
DEALLOCATE Chunks

' + CASE WHEN @PKisIdentity = 1 THEN N'SET IDENTITY_INSERT ' + @NewTableName + N' OFF;' ELSE N'' END + N'
GO'

END

EXEC #PrintMax @CMD

-- 6. Create non-clustered indexes on NEW

RAISERROR(N'	
/*************************************************************************
**************************************************************************

					!!! END OF PART 2 !!!

	The NEW table should be filled with most existing data at this point

**************************************************************************
**************************************************************************/


/*************************************************************************
**************************************************************************

					!!! BEGINNING PART 3 !!!

	The commands run in this section assume the NEW table already contains
	most of its data. Be sure to review it and make any changes as needed.
	(such as non-clustered indexes, foreign keys, check constraints etc.)

**************************************************************************
**************************************************************************/',0,1) WITH NOWAIT;

	SET @CMD = N''

	-- Generate nonclustered indexes
	SELECT @CMD = @CMD + N'

	CREATE NONCLUSTERED INDEX ' + QUOTENAME(i.name) + N' ON ' + @NewTableName + N'
	('
	+ STUFF(
		(SELECT ', ' + QUOTENAME(c.name) COLLATE database_default + ' ' + CASE is_descending_key WHEN 1 THEN 'DESC' ELSE 'ASC' END FROM sys.index_columns AS ic INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id WHERE ic.is_included_column = 0 AND ic.object_id = i.object_id AND ic.index_id = i.index_id ORDER BY ic.key_ordinal ASC FOR XML PATH(''))
		, 1,2,'')
	+ N') ' + ISNULL(N'
	INCLUDE (' +
		NULLIF(STUFF(
			(SELECT ', ' + QUOTENAME(c.name) COLLATE database_default FROM sys.index_columns AS ic INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id WHERE ic.is_included_column = 1 AND ic.object_id = i.object_id AND ic.index_id = i.index_id ORDER BY ic.key_ordinal ASC FOR XML PATH(''))
		, 1,2,''), N'')
		+ N')'
		, N'') + N'
	WITH (FILLFACTOR = ' + CONVERT(nvarchar(max), ISNULL(NULLIF(i.fill_factor,0),100))
			+ N', ALLOW_ROW_LOCKS = ' + CASE i.allow_row_locks WHEN 1 THEN 'ON' ELSE 'OFF' END
			+ N', ALLOW_PAGE_LOCKS = ' + CASE i.allow_page_locks WHEN 1 THEN 'ON' ELSE 'OFF' END
			+ N', IGNORE_DUP_KEY = ' + CASE i.ignore_dup_key WHEN 1 THEN 'ON' ELSE 'OFF' END
			+ N', PAD_INDEX = ' + CASE i.is_padded WHEN 1 THEN 'ON' ELSE 'OFF' END
	+ N')
	ON ' + QUOTENAME(ds.name) COLLATE database_default + N';'
	FROM sys.indexes AS i
	INNER JOIN sys.data_spaces AS ds ON i.data_space_id = ds.data_space_id 
	WHERE object_id = @SourceTableID
	AND i.name <> @CustomPKReplacementIndex
	AND i.index_id > 1 -- nonclustered

	-- Generate check constraints
	SET @CMD = @CMD + N'
GO'

	SELECT @CMD = @CMD + N'
	ALTER TABLE ' + @NewTableName + N' WITH NOCHECK ADD  CONSTRAINT ' + QUOTENAME(chk.name COLLATE database_default + @NewTableNamePostfix) + N' CHECK ' + chk.definition COLLATE database_default + N'
GO
	ALTER TABLE ' + @NewTableName + N' CHECK CONSTRAINT ' + QUOTENAME(chk.name COLLATE database_default + @NewTableNamePostfix) + N';
GO'
	FROM sys.check_constraints AS chk
	INNER JOIN sys.columns AS c
	ON chk.parent_object_id = c.object_id
	AND chk.parent_column_id = c.column_id
	WHERE chk.parent_object_id = @SourceTableID
	
EXEC #PrintMax @CMD

	-- TODO: user permissions

RAISERROR(N'	
/*************************************************************************
**************************************************************************

					!!! END OF PART 3 !!!

	The NEW table should be ready for the final merge at this point

**************************************************************************
**************************************************************************/


/*************************************************************************
**************************************************************************

					!!! BEGINNING PART 4 !!!

The next section assumes the NEW table is ready for the final stage.
The data collected in the DELTA table will be applied onto the NEW table,
And then the Critical Section will begin, where the OLD and NEW tables will
be renamed to switch places, and then another final merge from the DELTA
table will be performed to finalize the synchronization.


						!!! WARNING !!!
	
This is a reminder that if there were any column changes in the NEW table
(such as new columns, dropped or renamed columns), then you would need to
make changes accordingly in the following scripts!

**************************************************************************
**************************************************************************/',0,1) WITH NOWAIT;
PRINT 'GO'

-- Generate the merge command from the DETLA table onto the NEW table
-- This command will be executed twice:
--	once from the DELTA table to the NEW table BEFORE its name changes
--	and once from the DELTA table to the NEW table AFTER its name will be changed to the original table name
SET @CMD = N'
DECLARE @RCount INT
SELECT @RCount = COUNT(*) FROM ' + @DeltaTableName + N' WITH(NOLOCK);

RAISERROR(N''Merging %d rows from DELTA to NEW...'',0,1,@RCount) WITH NOWAIT;

' + CASE WHEN @PKisIdentity = 1 THEN N'SET IDENTITY_INSERT {@NewTableName} ON;' ELSE N'' END + N'

; WITH Trgt AS
(
	SELECT * FROM {@NewTableName} WITH(TABLOCKX)
), Delta AS
(
	SELECT Src.*
	FROM {@SourceTableName} AS Src
	INNER JOIN ' + @DeltaTableName + N' AS Trgt
	ON
		' + @PKjoin + N'
	WHERE Trgt.' + QUOTENAME(@OperationDeltaColumn) + N' = ''I''
)
MERGE INTO Trgt
USING Delta AS Src
ON
	' + @PKjoin + ISNULL(N'
WHEN MATCHED AND EXISTS
(
	SELECT Src.*
	EXCEPT
	SELECT Trgt.*
) THEN
	UPDATE SET
	' + @AllColumnsUpdateSet, N'') + N'
WHEN NOT MATCHED BY TARGET THEN
	INSERT (' + @AllColumnsList + N')
	VALUES (' + @AllColumnsList + N')
;
RAISERROR(N''Merged %d rows.'',0,1, @@ROWCOUNT) WITH NOWAIT;

' + CASE WHEN @PKisIdentity = 1 THEN N'SET IDENTITY_INSERT {@NewTableName} OFF;' ELSE N'' END
+ N'
DELETE Trgt
FROM {@NewTableName} AS Trgt
INNER JOIN ' + @DeltaTableName + N' AS Src
ON
	' + @PKjoin + N'
WHERE Src.' + QUOTENAME(@OperationDeltaColumn) + N' = ''D'';

RAISERROR(N''Deleted %d rows.'',0,1, @@ROWCOUNT) WITH NOWAIT;
'

DECLARE @CMD_Temp NVARCHAR(MAX)

-- 7. Perform the first merge from the DELTA table onto the NEW table

PRINT N'
RAISERROR(N''First Merge from DELTA Table into NEW Table...'',0,1) WITH NOWAIT;
'

SET @CMD_Temp = REPLACE(REPLACE(@CMD, N'{@NewTableName}',@NewTableName), N'{@SourceTableName}', @SourceTableName)

EXEC #PrintMax @CMD_Temp

-- 8. Rename tables, apply last DELTA changes, and drop the delta trigger
SET @CMD = N'
GO
/****************************************************************/

/********* !!! CRITICAL BLOCKING SECTION BEGINS NOW !!! *********/

/****************************************************************/

RAISERROR(N''Renaming tables...'',0,1) WITH NOWAIT;

SET XACT_ABORT ON;
BEGIN TRAN
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

EXEC sp_rename ''' + @SourceTableName + N''', ''' + @CleanSourceTableName + @OldTableNamePostfix + N'''
EXEC sp_rename ''' + @NewTableName + N''', ''' + @CleanSourceTableName + N'''
GO

RAISERROR(N''Final Merge from DELTA Table into NEW Table...'',0,1) WITH NOWAIT;

' + REPLACE(REPLACE(@CMD, N'{@NewTableName}',@SourceTableName), N'{@SourceTableName}', @OldTableName) + N'

RAISERROR(N''Dropping DELTA Trigger'',0,1) WITH NOWAIT;

DROP TRIGGER ' + QUOTENAME(OBJECT_SCHEMA_NAME(@SourceTableID)) + N'.' + QUOTENAME(@DeltaTriggerName) + N';
GO

/*************** Re-Creating Table Triggers ***************/

'

-- Generate triggers
SELECT @CMD = @CMD + N'
IF OBJECT_ID(N''' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N'.' + QUOTENAME(name COLLATE database_default) + N''') IS NOT NULL
DROP TRIGGER ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N'.' + QUOTENAME(name COLLATE database_default) + N';
GO
' + OBJECT_DEFINITION(object_id) COLLATE database_default + N'
GO
ALTER TABLE ' + @SourceTableName
+ CASE WHEN is_disabled = 1 THEN N' DISABLE' ELSE N' ENABLE' END + N' TRIGGER ' + QUOTENAME(name COLLATE database_default) + N';
GO'
FROM sys.triggers
WHERE parent_id = @SourceTableID

EXEC #PrintMax @CMD

-- Generate Rules 
SET @CMD = N'
/************* Re-Creating Table Rules ********************/';
	
SELECT
	@CMD = @CMD
	+ ISNULL(
            @vbCrLf
            + 'if exists(SELECT [name] FROM sys.objects WHERE TYPE=''R'' AND schema_id = ' + CONVERT(VARCHAR(30),[OBJS].[schema_id]) COLLATE database_default + ' AND [name] = '''  + QUOTENAME(OBJECT_NAME([COLS].[rule_object_id])) COLLATE database_default + ''')'  COLLATE database_default + @vbCrLf
			+ N'DROP RULE ' + QUOTENAME(OBJECT_SCHEMA_NAME([COLS].[rule_object_id])) + N'.' + QUOTENAME([COLS].[name]) COLLATE database_default + @vbCrLf + 'GO' +  @vbCrLf
            + [MODS].[definition] COLLATE database_default + @vbCrLf + 'GO' +  @vbCrLf
            + 'EXEC sp_binderule  ' + @SourceTableName + ', ''' + @SourceTableName + '.' + QUOTENAME([COLS].[name]) COLLATE database_default + '''' + @vbCrLf + 'GO'  COLLATE database_default ,'')
FROM [sys].[columns] [COLS] 
INNER JOIN [sys].[objects] [OBJS]
ON [OBJS].[object_id] = [COLS].[object_id]
INNER JOIN [sys].[sql_modules] [MODS]
ON [COLS].[rule_object_id] = [MODS].[object_id]
WHERE [COLS].[rule_object_id] <> 0
AND [COLS].[object_id] = @SourceTableID;
	
EXEC #PrintMax @CMD;


-- Generate Foreign Keys 
SET @CMD = N'
GO
/************* Re-Creating Foreign Keys ********************/';
	
SELECT
    @CMD = @CMD
    + @vbCrLf + [MyAlias].[Command] FROM
(
SELECT
  DISTINCT
  --FK must be added AFTER the PK/unique constraints are added back.
  850 AS [ExecutionOrder],
  'IF EXISTS (select * from sys.foreign_keys WHERE parent_object_id = OBJECT_ID(''' + @SourceTableName + N''') AND name = ''' + conz.name + N''')
   ALTER TABLE ' + @SourceTableName + N' DROP CONSTRAINT ' + QUOTENAME([conz].[name]) + N';' 
  + @vbCrLf
  + N'ALTER TABLE ' + @SourceTableName + N' ADD CONSTRAINT ' + QUOTENAME([conz].[name]) 
  + ' FOREIGN KEY (' 
  + [ChildCollection].[ChildColumns] 
  + ') REFERENCES ' 
  + QUOTENAME(SCHEMA_NAME([conz].[schema_id])) 
  + '.' 
  + QUOTENAME(OBJECT_NAME([conz].[referenced_object_id])) 
  + ' (' + [ParentCollection].[ParentColumns] 
  + ') ' 

  +  CASE [conz].[update_referential_action]
                                        WHEN 0 THEN '' --' ON UPDATE NO ACTION '
                                        WHEN 1 THEN ' ON UPDATE CASCADE '
                                        WHEN 2 THEN ' ON UPDATE SET NULL '
                                        ELSE ' ON UPDATE SET DEFAULT '
                                    END
                  + CASE [conz].[delete_referential_action]
                                        WHEN 0 THEN '' --' ON DELETE NO ACTION '
                                        WHEN 1 THEN ' ON DELETE CASCADE '
                                        WHEN 2 THEN ' ON DELETE SET NULL '
                                        ELSE ' ON DELETE SET DEFAULT '
                                    END
                  + CASE [conz].[is_not_for_replication]
                        WHEN 1 THEN ' NOT FOR REPLICATION '
                        ELSE ''
                    END
  + ';' AS [Command]
FROM   [sys].[foreign_keys] [conz]
       INNER JOIN [sys].[foreign_key_columns] [colz]
         ON [conz].[object_id] = [colz].[constraint_object_id]
      
       INNER JOIN (--gets my child tables column names   
SELECT
 [conz].[name],
 --technically, FK's can contain up to 16 columns, but real life is often a single column. coding here is for all columns
 [ChildColumns] = STUFF((SELECT 
                         ',' + QUOTENAME([REFZ].[name])
                       FROM   [sys].[foreign_key_columns] [fkcolz]
                              INNER JOIN [sys].[columns] [REFZ]
                                ON [fkcolz].[parent_object_id] = [REFZ].[object_id]
                                   AND [fkcolz].[parent_column_id] = [REFZ].[column_id]
                       WHERE [fkcolz].[parent_object_id] = [conz].[parent_object_id]
                           AND [fkcolz].[constraint_object_id] = [conz].[object_id]
                         ORDER  BY
                        [fkcolz].[constraint_column_id]
                      FOR XML PATH(''), TYPE).[value]('.','varchar(max)'),1,1,'')
FROM   [sys].[foreign_keys] [conz]
      INNER JOIN [sys].[foreign_key_columns] [colz]
        ON [conz].[object_id] = [colz].[constraint_object_id]
        WHERE [conz].[parent_object_id]= @SourceTableID
GROUP  BY
[conz].[name],
[conz].[parent_object_id],--- without GROUP BY multiple rows are returned
 [conz].[object_id]
    ) [ChildCollection]
         ON [conz].[name] = [ChildCollection].[name]
       INNER JOIN (--gets the parent tables column names for the FK reference
                  SELECT
                     [conz].[name],
                     [ParentColumns] = STUFF((SELECT
                                              ',' + [REFZ].[name]
                                            FROM   [sys].[foreign_key_columns] [fkcolz]
                                                   INNER JOIN [sys].[columns] [REFZ]
                                                     ON [fkcolz].[referenced_object_id] = [REFZ].[object_id]
                                                        AND [fkcolz].[referenced_column_id] = [REFZ].[column_id]
                                            WHERE  [fkcolz].[referenced_object_id] = [conz].[referenced_object_id]
                                              AND [fkcolz].[constraint_object_id] = [conz].[object_id]
                                            ORDER BY [fkcolz].[constraint_column_id]
                                            FOR XML PATH(''), TYPE).[value]('.','varchar(max)'),1,1,'')
                   FROM   [sys].[foreign_keys] [conz]
                          INNER JOIN [sys].[foreign_key_columns] [colz]
                            ON [conz].[object_id] = [colz].[constraint_object_id]
                           -- AND colz.parent_column_id 
                   GROUP  BY
                    [conz].[name],
                    [conz].[referenced_object_id],--- without GROUP BY multiple rows are returned
                    [conz].[object_id]
                  ) [ParentCollection]
         ON [conz].[name] = [ParentCollection].[name]
)[MyAlias];

EXEC #PrintMax @CMD;

SET @CMD = N'
GO
/*************** Re-Create Extended Properties ***************/'

  SELECT  @CMD =
          @CMD + @vbCrLf +
         'EXEC sys.sp_addextendedproperty
          @name = N'''  + [name] + ''', @value = N'''  + REPLACE(CONVERT(VARCHAR(MAX),[value]),'''','''''') + ''',
          @level0type = N''SCHEMA'', @level0name = ' + QUOTENAME(OBJECT_SCHEMA_NAME(@SourceTableID)) + ',
          @level1type = N''TABLE'', @level1name = '  + QUOTENAME(@CleanSourceTableName) + ';'
 --SELECT objtype, objname, name, value
  FROM [sys].[fn_listextendedproperty] (NULL, 'schema', OBJECT_SCHEMA_NAME(@SourceTableID), 'table', @CleanSourceTableName, NULL, NULL);
  --OMacoder suggestion for column extended properties http://www.sqlservercentral.com/Forums/FindPost1651606.aspx
   ;WITH [obj] AS (
	SELECT [split].[a].[value]('.', 'VARCHAR(20)') AS [name]
	FROM ( 
		SELECT CAST ('<M>' + REPLACE('column,constraint,index,trigger,parameter', ',', '</M><M>') + '</M>' AS XML) AS [data] 
		) AS [A] 
		CROSS APPLY [data].[nodes] ('/M') AS [split]([a])
	)
  SELECT 
  @CMD =
		 @CMD + @vbCrLf + @vbCrLf +
         'EXEC sys.sp_addextendedproperty
         @name = N''' COLLATE SQL_Latin1_General_CP1_CI_AS
         + [lep].[name] 
         + ''', @value = N''' + REPLACE(CONVERT(VARCHAR(MAX),[lep].[value]),'''','''''') + ''',
         @level0type = N''SCHEMA'', @level0name = ' + QUOTENAME(OBJECT_SCHEMA_NAME(@SourceTableID)) 
         + ', @level1type = N''TABLE'', @level1name = ' + QUOTENAME(@CleanSourceTableName) 
         + ', @level2type = N''' + UPPER([obj].[name]) + ''', @level2name = ' + QUOTENAME([lep].[objname]) + ';'
  --SELECT objtype, objname, name, value
  FROM [obj] 
	CROSS APPLY [sys].[fn_listextendedproperty] (NULL, 'schema', OBJECT_SCHEMA_NAME(@SourceTableID), 'table', @CleanSourceTableName, [obj].[name], NULL) AS [lep];  
  
  
EXEC #PrintMax @CMD;

SET @CMD = N'
GO
/*************** Finalizing ***************/

COMMIT TRAN

RAISERROR(N''Done. You can now drop the tables ' + @DeltaTableName + N' and ' + @OldTableName + N' and rename the constraints.'',0,1) WITH NOWAIT;
/*
DROP TABLE ' + @DeltaTableName + N';
DROP TABLE ' + @OldTableName + N';
GO
' + @RenameCommands + N'
*/
GO
'

EXEC #PrintMax @CMD

Quit:
GO
IF OBJECT_ID('tempdb..#PrintMax', 'P') IS NOT NULL DROP PROCEDURE #PrintMax;