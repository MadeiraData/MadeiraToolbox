/*========================================================================================================================
Description:	This query finds all the query plans currently in cache, which include a specific physical operator
				(for example: "Clustered Index Seek" on a specific index
Scope:			Instance
Author:			Guy Glantser, Madeira
Created:		13/11/2014
Last Updated:	13/11/2014
Notes:			Replace the local variable values with your choices
=========================================================================================================================*/

DECLARE
	@DatabaseName		AS SYSNAME		= N'eDate' ,
	@SchemaName			AS SYSNAME		= N'Operation' ,
	@TableName			AS SYSNAME		= N'Members' ,
	@IndexName			AS SYSNAME		= N'pk_Members_c_Id' ,
	@PhysicalOperator	AS NVARCHAR(50)	= N'Clustered Index Seek';

SET @DatabaseName	= QUOTENAME (@DatabaseName , N'[');
SET @SchemaName		= QUOTENAME (@SchemaName , N'[');
SET @TableName		= QUOTENAME (@TableName , N'[');
SET @IndexName		= QUOTENAME (@IndexName , N'[');

WITH
	XMLNAMESPACES (DEFAULT N'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
SELECT
	QueryPlan		= QueryPlans.query_plan ,
	PlanHandle		= CachedPlans.plan_handle ,
	StatementText	= StatementNode.query (N'.').value (N'StmtSimple[1]/@StatementText' , N'NVARCHAR(MAX)')
FROM
	sys.dm_exec_cached_plans AS CachedPlans
CROSS APPLY
	sys.dm_exec_query_plan (CachedPlans.plan_handle) AS QueryPlans
CROSS APPLY
	QueryPlans.query_plan.nodes (N'//StmtSimple') AS Statements (StatementNode)
CROSS APPLY
	StatementNode.nodes (N'//RelOp[@PhysicalOp = sql:variable("@PhysicalOperator")]') AS Operators (OperatorNode)
CROSS APPLY
	OperatorNode.nodes (N'IndexScan/Object[@Database = sql:variable("@DatabaseName")][@Schema = sql:variable("@SchemaName")][@Table = sql:variable("@TableName")][@Index = sql:variable("@IndexName")]') AS OperatorObjects (OperatorObject)
WHERE
	CachedPlans.cacheobjtype = N'Compiled Plan';
GO
