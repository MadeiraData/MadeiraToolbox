/*

Query Template Forced Parameterization
======================================

Author:			Guy Glantser, https://www.madeiradata.com
Date:			18/07/2023
Description:
	This script creates a template plan guide for a specific query
	to apply forced parameterization.
*/

DECLARE
	@Statement	AS NVARCHAR(MAX) ,
	@Params		AS NVARCHAR(MAX);

EXECUTE sys.sp_get_query_template
	@querytext		=
		N'
			Your query goes here...
		' ,
	@templatetext	= @Statement	OUTPUT ,
	@parameters		= @Params		OUTPUT;

EXECUTE sys.sp_create_plan_guide
	@name				= N'PlanGuideName' ,
	@stmt				= @Statement ,
	@type				= N'TEMPLATE' ,
	@module_or_batch	= NULL ,
	@params				= @Params ,
	@hints				= N'OPTION (PARAMETERIZATION FORCED)';
GO
