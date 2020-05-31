/*========================================================================================================================

Description:	Count the number of execution references to each view in the current database
Scope:			Database
Author:			Guy Glantser
Created:		27/05/2014
Last Updated:	27/05/2014
Notes:			Based on the following forum thread: http://social.msdn.microsoft.com/Forums/he-IL/9423fa68-2ab6-414a-8d04-2b192c207f46/-views?forum=sqlhe.
				This query returns all the views in the current database, but it counts the number of execution references in the whole instance, meaning if there are two views
				in two distinct databases with the same name, the query will return a single row with the total count from both databases.
				The query searches only by the view name, regardless of the schema. If there are two different views in the current database in different schema, then the query
				will return a row for each view with the total count in both rows (same value).

=========================================================================================================================*/


WITH
	QueryExecutions
AS
	(
		SELECT
			QueryText	=
				SUBSTRING
				(
					SQLTexts.text						  ,
					QueryStats.statement_start_offset / 2 ,
					(
						CASE QueryStats.statement_end_offset
							WHEN -1 THEN LEN (SQLTexts.text)
							ELSE QueryStats.statement_end_offset / 2
						END
						- QueryStats.statement_start_offset / 2
					)
					+ 1
				) ,
			ExecutionCount	= QueryStats.execution_count
		FROM
			sys.dm_exec_query_stats AS QueryStats
		CROSS APPLY
			sys.dm_exec_sql_text (QueryStats.sql_handle) AS SQLTexts
	)
SELECT
	ViewSchema	= SCHEMA_NAME (Views.schema_id) ,
	ViewName	= Views.name ,
	UsageCount	= SUM (ISNULL (QueryExecutions.ExecutionCount , 0))
FROM
	sys.views AS Views
LEFT OUTER JOIN
	QueryExecutions
ON
	QueryExecutions.QueryText LIKE N'%[. ' + CHAR(9) + CHAR(10) + CHAR(13) + N'\[]' + Views.name + N'[ ;' + CHAR(9) + CHAR(10) + CHAR(13) + '\]]%' ESCAPE N'\'
GROUP BY
	SCHEMA_NAME (Views.schema_id) ,
	Views.name
ORDER BY
	ViewSchema	ASC ,
	ViewName	ASC;
GO
