SELECT
  [Schema] = s.name,
  [Table] = o.name,
  [Constraint] = i.name,
  [Type] = i.const_type,
  RemediationCmd = CASE WHEN CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5 THEN N'' ELSE N'USE ' + DB_NAME() + N'; ' END
	+ N'BEGIN TRY ALTER TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N' WITH CHECK CHECK CONSTRAINT ' + QUOTENAME(i.name) + N'; END TRY BEGIN CATCH PRINT ERROR_MESSAGE(); END CATCH'
FROM 
(
	SELECT
		parent_object_id,
		is_not_trusted,
		name,
		'CHK' AS const_type
	FROM
		sys.check_constraints cc
	WHERE
		is_not_trusted = 1
	
	UNION ALL

	SELECT
		parent_object_id,
		is_not_trusted,
		name,
		'FK' AS const_type
	FROM
		sys.foreign_keys fk		
	WHERE
		is_not_trusted = 1
) AS i
INNER JOIN sys.objects o ON i.parent_object_id = o.object_id 
INNER JOIN sys.schemas s ON o.schema_id = s.schema_id