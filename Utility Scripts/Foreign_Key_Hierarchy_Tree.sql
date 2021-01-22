/*
Retrieve Foreign Key Hierarchy Tree
===================================
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date: 2021-01-07
Description:
Retrieve the hierarchy tree for a given table,
based on foreign key references.
Use this script to map out your entity relational structure,
see which foreign keys are dependent on a given table,
their names, and various constraint properties.

Arguments:
===========
@TableName		- Table name used as the initial "point of entry". Hierarchy tree will extend from this "root" table.
@MaxLevel		- Used for avoiding exaggerated recursion depth.
@AvoidInfiniteRecurse	- Uses the hierarchyid data type to avoid infinite recursion loop.
@CascadeOnly		- Set to 1 to only see hierarchy paths with either CASCADE or SET NULL options.
===================================
*/
DECLARE
	@TableName		SYSNAME	= 'dbo.Catalog',
	@MaxLevel		INT	= 100,
	@AvoidInfiniteRecurse	BIT	= 0,
	@CascadeOnly		BIT	= 0

SET NOCOUNT ON;
WITH Tree
AS
(
SELECT 1 AS lvl, fk.[name], fk.[object_id], fk.[parent_object_id], fk.[referenced_object_id]
, fk.delete_referential_action, fk.update_referential_action
, fk.is_not_trusted, fk.is_disabled, fk.is_not_for_replication
, hid = CAST('/' + CONVERT(varchar, fk.[referenced_object_id]) + '/' + CONVERT(varchar, fk.[parent_object_id]) + '/' AS hierarchyid)
FROM sys.foreign_keys AS fk
WHERE fk.referenced_object_id = OBJECT_ID(@TableName)
AND (ISNULL(@CascadeOnly,0) = 0 OR fk.delete_referential_action > 0 OR fk.update_referential_action > 0)

UNION ALL

SELECT Tree.lvl + 1 AS lvl, fk.[name], fk.[object_id], fk.[parent_object_id], fk.[referenced_object_id]
, fk.delete_referential_action, fk.update_referential_action
, fk.is_not_trusted, fk.is_disabled, fk.is_not_for_replication
, hid = CAST(Tree.hid.ToString() + CONVERT(varchar, fk.[parent_object_id]) + '/' AS hierarchyid)
FROM sys.foreign_keys AS fk
INNER JOIN Tree ON fk.referenced_object_id = Tree.parent_object_id
WHERE Tree.parent_object_id <> Tree.referenced_object_id
AND (@AvoidInfiniteRecurse = 0 OR Tree.hid.ToString() NOT LIKE '%/' + CONVERT(varchar,fk.[parent_object_id]) + '/%')
AND (@MaxLevel IS NULL OR Tree.lvl < @MaxLevel)
AND (ISNULL(@CascadeOnly,0) = 0 OR fk.delete_referential_action > 0 OR fk.update_referential_action > 0)
)
SELECT
  lvl = MIN(lvl)
, foreign_key_name		= [name]
, table_name			= QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(parent_object_id))
, referenced_table_name 	= QUOTENAME(OBJECT_SCHEMA_NAME(referenced_object_id)) + '.' + QUOTENAME(OBJECT_NAME(referenced_object_id))
, on_delete_action		= delete_referential_action
, on_update_action		= update_referential_action
, is_not_trusted
, is_disabled
, is_not_for_replication
FROM Tree
GROUP BY
  [name]
, parent_object_id, referenced_object_id
, delete_referential_action, update_referential_action
, is_not_trusted, is_disabled, is_not_for_replication
ORDER BY 1, 2, 3, 4