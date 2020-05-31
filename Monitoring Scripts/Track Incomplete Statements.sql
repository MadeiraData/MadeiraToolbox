/*========================================================================================================================

Description:	Create an event session that tracks statements which have never completed
Scope:			Instance
Author:			Guy Glantser
Created:		15/10/2013
Last Updated:	15/10/2013
Notes:			Based on a post by Jonathan Kehayias
				(http://sqlblog.com/blogs/jonathan_kehayias/archive/2010/12/09/an-xevent-a-day-9-of-31-targets-week-pair-matching.aspx)

=========================================================================================================================*/


CREATE EVENT SESSION
	IncompleteStatements
ON
	SERVER
ADD EVENT
	sqlserver.sql_statement_starting
(
	ACTION
		(
			sqlserver.session_id ,
			sqlserver.tsql_stack
		)
) ,
ADD EVENT
	sqlserver.sql_statement_completed
(
	ACTION
		(
			sqlserver.session_id ,
			sqlserver.tsql_stack
		)
)
ADD TARGET
	package0.pair_matching
		(
			SET
				begin_event				= N'sqlserver.sql_statement_starting' ,
				begin_matching_actions	= N'sqlserver.session_id , sqlserver.tsql_stack' ,
				end_event				= N'sqlserver.sql_statement_completed' ,
				end_matching_actions	= N'sqlserver.session_id , sqlserver.tsql_stack'
		);
GO


ALTER EVENT SESSION
	IncompleteStatements
ON
	SERVER
STATE = START;
GO


DROP EVENT SESSION
	IncompleteStatements
ON
	SERVER;
GO
