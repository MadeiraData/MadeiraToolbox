/*========================================================================================================================

Description:	Check for latch contention on allocation pages in "tempdb"
Scope:			Instance
Author:			Robert Davis (http://www.sqlservercentral.com/blogs/robert_davis/2010/03/05/Breaking-Down-TempDB-Contention/)
Created:		05/03/2010
Last Updated:	28/07/2013
Notes:			If there is high contention on allocation pages in tempdb,
				then consider increasing the number of data files for tempdb.
				See this post for more information: http://www.sqlskills.com/blogs/paul/a-sql-server-dba-myth-a-day-1230-tempdb-should-always-have-one-data-file-per-processor-core/

=========================================================================================================================*/


SELECT
	SessionId			= session_id ,
	WaitType			= wait_type ,
	WaitDuration_MS		= wait_duration_ms ,
	BlockingSessionId	= blocking_session_id ,
	ResourceDescription	= resource_description ,
	ResourceType		=
		CASE
			WHEN
				CAST (RIGHT (resource_description , LEN (resource_description) - CHARINDEX (N':' , resource_description , 3)) AS INT) - 1 % 8088 = 0
			THEN
				N'PFS Page'
			WHEN
				CAST (RIGHT (resource_description , LEN (resource_description) - CHARINDEX (N':' , resource_description , 3)) AS INT) - 2 % 511232 = 0
			THEN
				N'GAM Page'
			WHEN
				CAST (RIGHT (resource_description , LEN (resource_description) - CHARINDEX (N':' , resource_description , 3)) AS INT) - 3 % 511232 = 0
			THEN
				N'SGAM Page'
			ELSE
				N'Other (Not PFS, GAM or SGAM page)'
		END
FROM
	sys.dm_os_waiting_tasks
WHERE
	wait_type LIKE N'PAGE%LATCH_%'	-- Only "PAGELATCH" or "PAGEIOLATCH" wait types
AND
	resource_description LIKE N'2:%';	-- Only in "tempdb"
GO
