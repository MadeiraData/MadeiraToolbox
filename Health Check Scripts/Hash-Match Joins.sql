/*========================================================================================================================

Description:	Display information about hash-match joins in the current database
Scope:			Database
Author:			Guy Glantser, Madeira Data Solutions
Created:		29/01/2024
Last Updated:	29/01/2024

Notes:
				1. This query extracts data from the Query Store. Make sure that the Query Store is active in the current database and has sufficient data.
				2. This query only displays information about hash-match joins. It's possible to retrieve information about merge joins with minimal changes.
				3. Retrieving information about nested-loops joins is a lot more complicated.
				4. In case of multi-column joins, each pair of column will be displayed in a separate row with a different JoinColumnOrdinalPosition.

=========================================================================================================================*/

USE
	DatabaseName;
GO


WITH
	XMLNAMESPACES (DEFAULT N'http://schemas.microsoft.com/sqlserver/2004/07/showplan') ,

	DistinctExecutionPlans
(
	QueryPlanHash ,
	QueryHash ,
	QueryPlan ,
	LastExecutionTime
)
AS
(
	SELECT
		QueryPlanHash		= QueryPlans.query_plan_hash ,
		QueryHash			= Queries.query_hash ,
		QueryPlan			= TRY_CAST (MIN (QueryPlans.query_plan) AS XML) ,
		LastExecutionTime	= MAX (QueryPlans.last_execution_time)
	FROM
		sys.query_store_plan AS QueryPlans
	INNER JOIN
		sys.query_store_query AS Queries
	ON
		QueryPlans.query_id = Queries.query_id
	GROUP BY
		QueryPlans.query_plan_hash ,
		Queries.query_hash
	HAVING
		TRY_CAST (MIN (QueryPlans.query_plan) AS XML) IS NOT NULL
) ,

	HashJoinOperators
(
	QueryPlanHash ,
	QueryHash ,
	LastExecutionTime ,
	NodeId ,
	JoinOperator
)
AS
(
	SELECT
		QueryPlanHash		= DistinctExecutionPlans.QueryPlanHash ,
		QueryHash			= DistinctExecutionPlans.QueryHash ,
		LastExecutionTime	= DistinctExecutionPlans.LastExecutionTime ,
		NodeId				= JoinOperators.JoinOperator.value ('./../@NodeId' , 'INT') ,
		JoinOperator		= JoinOperators.JoinOperator.query ('.')
	FROM
		DistinctExecutionPlans
	CROSS APPLY
		DistinctExecutionPlans.QueryPlan.nodes ('(//RelOp)[contains(@LogicalOp,"Join")]/Hash') AS JoinOperators (JoinOperator)
) ,

	OuterColumns
(
	QueryPlanHash ,
	QueryHash ,
	NodeId ,
	DatabaseName ,
	SchemaName ,
	TableName ,
	ColumnName ,
	OrdinalPosition
)
AS
(
	SELECT
		QueryPlanHash	= HashJoinOperators.QueryPlanHash ,
		QueryHash		= HashJoinOperators.QueryHash ,
		NodeId			= HashJoinOperators.NodeId ,
		DatabaseName	= OuterColumns.ColumnReference.value('(./@Database)[1]' , 'SYSNAME') ,
		SchemaName	 	= OuterColumns.ColumnReference.value('(./@Schema)[1]' , 'SYSNAME') ,
		TableName	 	= OuterColumns.ColumnReference.value('(./@Table)[1]' , 'SYSNAME') ,
		ColumnName	 	= OuterColumns.ColumnReference.value('(./@Column)[1]' , 'SYSNAME') ,
		OrdinalPosition	= OuterColumns.ColumnReference.value('let $i := . return count(../ColumnReference[. << $i]) + 1' , 'INT')
	FROM
		HashJoinOperators
	CROSS APPLY
		HashJoinOperators.JoinOperator.nodes ('/Hash/HashKeysBuild/ColumnReference') AS OuterColumns (ColumnReference)
) ,

	InnerColumns
(
	QueryPlanHash ,
	QueryHash ,
	NodeId ,
	DatabaseName ,
	SchemaName ,
	TableName ,
	ColumnName ,
	OrdinalPosition
)
AS
(
	SELECT
		QueryPlanHash	= HashJoinOperators.QueryPlanHash ,
		QueryHash		= HashJoinOperators.QueryHash ,
		NodeId			= HashJoinOperators.NodeId ,
		DatabaseName	= InnerColumns.ColumnReference.value('(./@Database)[1]' , 'SYSNAME') ,
		SchemaName	 	= InnerColumns.ColumnReference.value('(./@Schema)[1]' , 'SYSNAME') ,
		TableName	 	= InnerColumns.ColumnReference.value('(./@Table)[1]' , 'SYSNAME') ,
		ColumnName	 	= InnerColumns.ColumnReference.value('(./@Column)[1]' , 'SYSNAME') ,
		OrdinalPosition	= InnerColumns.ColumnReference.value('let $i := . return count(../ColumnReference[. << $i]) + 1' , 'INT')
	FROM
		HashJoinOperators
	CROSS APPLY
		HashJoinOperators.JoinOperator.nodes ('/Hash/HashKeysProbe/ColumnReference') AS InnerColumns (ColumnReference)
) ,

	FinalJoins
(
	QueryPlanHash ,
	QueryHash ,
	NodeId ,
	OrdinalPosition ,
	OuterDatabaseName ,
	OuterSchemaName ,
	OuterTableName ,
	OuterColumnName ,
	InnerDatabaseName ,
	InnerSchemaName ,
	InnerTableName ,
	InnerColumnName
)
AS
(
	SELECT
		QueryPlanHash		= OuterColumns.QueryPlanHash ,
		QueryHash			= OuterColumns.QueryHash ,
		JoinOperator		= OuterColumns.NodeId ,
		OrdinalPosition		= OuterColumns.OrdinalPosition ,
		OuterDatabaseName	= OuterColumns.DatabaseName ,
		OuterSchemaName	 	= OuterColumns.SchemaName ,
		OuterTableName	 	= OuterColumns.TableName ,
		OuterColumnName	 	= OuterColumns.ColumnName ,
		InnerDatabaseName	= InnerColumns.DatabaseName ,
		InnerSchemaName	 	= InnerColumns.SchemaName ,
		InnerTableName	 	= InnerColumns.TableName ,
		InnerColumnName	 	= InnerColumns.ColumnName
	FROM
		OuterColumns
	INNER JOIN
		InnerColumns
	ON
		OuterColumns.QueryPlanHash = InnerColumns.QueryPlanHash
	AND
		OuterColumns.QueryHash = InnerColumns.QueryHash
	AND
		OuterColumns.NodeId = InnerColumns.NodeId
	AND
		OuterColumns.OrdinalPosition = InnerColumns.OrdinalPosition
	WHERE
		OuterColumns.SchemaName != N'[sys]'
	AND
		InnerColumns.SchemaName != N'[sys]'
	AND
		OuterColumns.DatabaseName != '[tempdb]'
	AND
		InnerColumns.DatabaseName != '[tempdb]'
)

SELECT
	QueryPlanId					= DENSE_RANK () OVER (ORDER BY DistinctExecutionPlans.QueryPlanHash ASC , DistinctExecutionPlans.QueryHash ASC) ,
	QueryPlan					= DistinctExecutionPlans.QueryPlan ,
	LastExecutionTime			= DistinctExecutionPlans.LastExecutionTime ,
	NodeId						= FinalJoins.NodeId ,
	JoinColumnOrdinalPosition	= FinalJoins.OrdinalPosition ,
	OuterDatabaseName			= FinalJoins.OuterDatabaseName ,
	OuterSchemaName	 			= FinalJoins.OuterSchemaName ,
	OuterTableName	 			= FinalJoins.OuterTableName ,
	OuterColumnName	 			= FinalJoins.OuterColumnName ,
	InnerDatabaseName			= FinalJoins.InnerDatabaseName ,
	InnerSchemaName	 			= FinalJoins.InnerSchemaName ,
	InnerTableName	 			= FinalJoins.InnerTableName ,
	InnerColumnName	 			= FinalJoins.InnerColumnName
FROM
	DistinctExecutionPlans
INNER JOIN
	FinalJoins
ON
	DistinctExecutionPlans.QueryPlanHash = FinalJoins.QueryPlanHash
AND
	DistinctExecutionPlans.QueryHash = FinalJoins.QueryHash
ORDER BY
	QueryPlanId					ASC ,
	NodeId						ASC ,
	JoinColumnOrdinalPosition	ASC;
GO
