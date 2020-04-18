/*========================================================================================================================

Description: 	Script that automaticly rebuilds or reorganizes the indexes according to user defined thresholds.
				The fill factor can be reset or kept the same. Old and new frag data is dispalyed at the end.
				
Scope:			Instance

Author:			Madeira

Created:		19/01/2012

Type: 			Script Plug&play Must supply DB name at line 27! Optional Modify include: Fragmentation threshold for rebuild in
				line 29. Fragmentation threshold for reorganize (@MinRebuild >= @MinReorg) in line 30. In addition, you can
				Set desired fill factor for rebuild. 0 to keep the existing fill factor in line 31.
				
Notes: 			Don't forget to delete the temporary. 

Warnings: 		In older version of SQL you can't rebuild online! be aware of that. 

=========================================================================================================================*/


DECLARE
 @DBName  SYSNAME = NULL , -- Enter a DB name for a specific DB, or NULL for all DBs. <-- 
 @MinRebuild DECIMAL = 30.0 , -- Fragmentation threshold for rebuild. <-- 
 @MinReorg DECIMAL = 10.0 , -- Fragmentation threshold for reorganize (@MinRebuild >= @MinReorg). <--
 @FillFactor TINYINT = 0 , -- Set desired fill factor for rebuild. 0 to keep the existing fill factor <--
 @SchemaName SYSNAME ,
 @TableName SYSNAME ,
 @IndexName SYSNAME ,
 @Frag  DECIMAL(4,1) ,
 @Statment NVARCHAR(MAX);

-- Create a table for all of the indexes
CREATE TABLE
 #fraglist
(
 DBId  SMALLINT NOT NULL ,
 DBName  SYSNAME  NOT NULL ,
 ObjectId INT   NOT NULL ,
 SchemaName SYSNAME  NOT NULL ,
 TableName SYSNAME  NOT NULL ,
 IndexId  INT   NOT NULL ,
 IndexName SYSNAME  NOT NULL ,
 Frag  DECIMAL(4,1)NOT NULL ,
 Depth  TINYINT  NOT NULL
);


IF @DBName IS NULL
BEGIN -- A list of all DBs
 DECLARE
  CUR_databases CURSOR FAST_FORWARD
 FOR SELECT
  name
 FROM
  sys.databases
 WHERE
  name NOT IN (N'master', N'tempdb', N'model', N'msdb', N'distributor')
  AND 
  source_database_id IS NULL ;
END

ELSE
BEGIN -- Only the selected DB
 DECLARE
  CUR_databases CURSOR FAST_FORWARD
 FOR SELECT
  @DBName
END

OPEN CUR_databases;

-- Loop through all the databases.
FETCH NEXT FROM
 CUR_databases
INTO
 @DBName;

WHILE @@FETCH_STATUS = 0
BEGIN
 INSERT INTO
  #fraglist
 EXECUTE
 (N'
  SELECT
   DB_ID(N''' + @DBName + N''') ,
   N''' + @DBName + N''' ,
   Stats.object_id ,
   schemas.name ,
   objects.name ,
   Stats.index_id ,
   indexes.name ,
   Stats.avg_fragmentation_in_percent ,
   Stats.index_depth
  FROM
   sys.dm_db_index_physical_stats (DB_ID(N''' + @DBName + N'''), NULL, NULL, NULL, NULL) AS Stats
  INNER JOIN
   [' + @DBName + N'].sys.objects
   ON Stats.object_id = objects.object_id
  INNER JOIN
   [' + @DBName + N'].sys.schemas
   ON objects.schema_id = schemas.schema_id
  INNER JOIN
   [' + @DBName + N'].sys.indexes
   ON Stats.index_id = indexes.index_id
   AND Stats.object_id = indexes.object_id
  WHERE
   indexes.name IS NOT NULL;');

 FETCH NEXT FROM
  CUR_databases
 INTO
  @DBName;
END;

-- Close and deallocate the cursor.
CLOSE CUR_databases;
DEALLOCATE CUR_databases;

-- A list of indexes to be defragged.
DECLARE CUR_indexes CURSOR
FOR SELECT
 DBName ,
 SchemaName ,
 TableName ,
 IndexName ,
 Frag
FROM
 #fraglist
WHERE
 Frag >= @MinReorg
 AND
 Depth > 0;

-- Open the cursor.
OPEN CUR_indexes;

-- Loop through the indexes.
FETCH NEXT FROM
 CUR_indexes
INTO
 @DBName ,
 @SchemaName ,
 @TableName ,
 @IndexName ,
 @Frag;

WHILE @@FETCH_STATUS = 0
BEGIN
 IF @Frag >= @MinRebuild -- Rebuild 
 BEGIN
  PRINT
  N'
   Rebuilding ' + @IndexName + N' on [' + @DBName + N'].[' + @SchemaName + N'].[' + @TableName + N'].
   Fill Factor will ' + (CASE WHEN @FillFactor > 0
   THEN N'be set to: ' + CAST(@FillFactor AS NVARCHAR(3)) + N'' ELSE N'not change' END) + N'.
   Current fragmentation: ' + CAST(@Frag AS NVARCHAR(5)) + N'%.';
  
  SET @Statment =
  N'
   ALTER INDEX
    [' + @IndexName + N']
   ON
    [' + @DBName + N'].[' + @SchemaName + N'].[' + @TableName + N']
   REBUILD
   ' + (CASE WHEN @FillFactor > 0
   THEN N'WITH (FILLFACTOR = ' + CAST(@FillFactor AS NVARCHAR(3)) + N');' ELSE N';' END);
  
  END
 ELSE -- Reorganize
 BEGIN
  PRINT
  N'
   Reorganizing [' + @IndexName + N'] on [' + @DBName + N'].[' + @SchemaName + N'].[' + @TableName + N'].
   Current fragmentation: ' + CAST(@Frag AS NVARCHAR(5)) + N'%.';
  
  SET @Statment =
  N'
   ALTER INDEX
    [' + @IndexName + N']
   ON
    [' + @DBName + N'].[' + @SchemaName + N'].[' + @TableName + N']
   REORGANIZE;';
 END;
 
 EXECUTE sys.sp_executesql
  @Statment;
 
 FETCH NEXT FROM
  CUR_indexes
 INTO
  @DBName ,
  @SchemaName ,
  @TableName ,
  @IndexName ,
  @Frag;
END;

-- Close and deallocate the cursor.
CLOSE CUR_indexes;
DEALLOCATE CUR_indexes;

-- Display new fragmentation.
SELECT
 DBName   = OLD.DBName ,
 TableName  = OLD.SchemaName + N'.' + OLD.TableName ,
 IndexName  = OLD.IndexName ,
 OldFrag   = OLD.Frag ,
 NewFrag   = CAST(NEW.avg_fragmentation_in_percent AS DECIMAL(4,1)) ,
 IndexPageCount = NEW.page_count
FROM
 #fraglist AS OLD
INNER JOIN
 sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, NULL) AS NEW
 ON OLD.DBId = NEW.database_id
 AND OLD.ObjectId = NEW.object_id
 AND OLD.IndexId = NEW.index_id
ORDER BY
 DBName ,
 Frag DESC;

-- Drop the temporary table.
DROP TABLE
 #fraglist;
GO