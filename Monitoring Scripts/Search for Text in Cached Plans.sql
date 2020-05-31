/*========================================================================================================================

Description:	Search for text in cached plans
Scope:			Instance
Author:			Ian Stirk (http://www.sqlservercentral.com/articles/Performance+Tuning/66729/)
Created:		22/06/2012
Last Updated:	13/02/2014
Notes:			

=========================================================================================================================*/


DECLARE
	@StringToSearchFor AS NVARCHAR(MAX) = N'TableScan';

SELECT TOP (20)
	BatchText		= BatchTexts.text ,
	BatchPlan		= QueryPlans.query_plan ,
	CacheObjectType	= CachedPlans.cacheobjtype ,
	ObjectType		= CachedPlans.objtype ,
	DatabaseName	= DB_NAME (BatchTexts.dbid) ,
	UseCount		= CachedPlans.usecounts
FROM
	sys.dm_exec_cached_plans AS CachedPlans
CROSS APPLY
	sys.dm_exec_sql_text (CachedPlans.plan_handle) AS BatchTexts
CROSS APPLY
	sys.dm_exec_query_plan (CachedPlans.plan_handle) AS QueryPlans
WHERE
	CAST (QueryPlans.query_plan AS NVARCHAR(MAX)) LIKE N'%' + @StringToSearchFor + N'%'
ORDER BY
	UseCount DESC;
GO
