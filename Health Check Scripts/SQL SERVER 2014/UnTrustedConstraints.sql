SELECT
  s.name + '.' + o.name + '.' + i.name  AS [Constraint Not trusted]
FROM 
(
	SELECT
		parent_object_id,
		is_not_trusted,
		name
	FROM
		sys.check_constraints cc
	
	UNION ALL

	SELECT
		parent_object_id,
		is_not_trusted,
		name
	FROM
		sys.foreign_keys fk		
) AS i
INNER JOIN 
	sys.objects o 
	ON i.parent_object_id = o.object_id 
INNER JOIN sys.schemas s 
	ON o.schema_id = s.schema_id 
WHERE i.is_not_trusted = 1
