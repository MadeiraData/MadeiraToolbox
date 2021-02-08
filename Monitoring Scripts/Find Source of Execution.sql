/*========================================================================================================================
Description:	This script shows the source of each connection. By uncommenting the last where clause it can 
				be used to identify what/who is executing something.
				It returns a job name when being executed from a job, or the sql text if not.
				Code inspired from this post:
				https://social.msdn.microsoft.com/Forums/sqlserver/en-US/59db3794-0346-4a8d-97f3-741fcac47a26/determine-sql-agent-job-name-from-spid?forum=transactsql
Scope:			Instance
Author:			Sagiu Amichai, Madeira
Created:		08/02/2021
Last Updated:	08/02/2021
Notes:			Replace the local variable values with your choices
=========================================================================================================================*/


SELECT 	
	des.session_id,
	des.login_name,
	des.program_name,	
	isnull(
		jb.SQL_Job_name, 
		t.text 
	) ExecutionSource	
FROM	
		sys.dm_exec_requests der
INNER JOIN 
		sys.dm_Exec_sessions des
ON 
		der.session_id=des.session_id
CROSS APPLY
		sys.dm_exec_sql_text(der.sql_handle) t
OUTER APPLY
(
	SELECT  name as SQL_Job_name 
	FROM msdb.dbo.sysjobs sj
	WHERE substring ((cast(sj.job_id as varchar(36))),7,2) +
		substring ((cast(sj.job_id as varchar(36))),5,2)+
		substring ((cast(sj.job_id as varchar(36))),3,2)+
		substring ((cast(sj.job_id as varchar(36))),1,2)+
		substring ((cast(sj.job_id as varchar(36))),12,2)+
		substring ((cast(sj.job_id as varchar(36))),10,2)+
		substring ((cast(sj.job_id as varchar(36))),17,2)+
		substring ((cast(sj.job_id as varchar(36))),15,2)+
		substring ((cast(sj.job_id as varchar(36))),20,4)+
		substring ((cast(sj.job_id as varchar(36))),25,12)
		=substring((cast(des.program_name as varchar(75))),32,32)
) jb
--WHERE des.session_id = @@SPID

