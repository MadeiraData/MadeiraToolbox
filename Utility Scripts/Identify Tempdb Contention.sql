/*========================================================================================================================

Description:	Display information about contention in tempdb, if currently exists
Scope:			Instance
Author:			Guy Glantser
Created:		09/05/2022
Last Updated:	09/05/2022
Notes:			

=========================================================================================================================*/

WITH
	WaitingTasks
(
	SessionId ,
	WaitType ,
	WaitDuration_MS ,
	BlockingSessionId ,
	ResourceDescription ,
	FileId ,
    PageId
)
AS
(
	SELECT
		SessionId			= session_id ,
		WaitType			= wait_type ,
		WaitDuration_MS		= wait_duration_ms ,
		BlockingSessionId	= blocking_session_id ,
		ResourceDescription	= resource_description ,
		FileId				= CAST (SUBSTRING (resource_description , 3 , CHARINDEX (N':' , resource_description , 3) - 3) AS INT) ,
        PageId				= CAST (RIGHT (resource_description , LEN (resource_description) - CHARINDEX (N':' , resource_description , 3)) AS INT)
	FROM
		sys.dm_os_waiting_tasks
	WHERE
		wait_type LIKE N'PAGE%LATCH_%'
	AND
		resource_description LIKE N'2:%'
)
SELECT
	SessionId			= WaitingTasks.SessionId ,
	WaitType			= WaitingTasks.WaitType ,
	WaitDuration_MS		= WaitingTasks.WaitDuration_MS ,
	BlockingSessionId	= WaitingTasks.BlockingSessionId ,
	ResourceDescription	= WaitingTasks.ResourceDescription ,
	ContentionType		=
		CASE
			WHEN WaitingTasks.PageID = 1
			OR WaitingTasks.PageID % 8088 = 0
			OR WaitingTasks.PageID = 2
			OR WaitingTasks.PageID % 511232 = 0
			OR WaitingTasks.PageID = 3
			OR (WaitingTasks.PageID - 1) % 511232 = 0
				THEN N'Object Allocation Contention'
			WHEN TempdbObjects.[type] = 'S'
				THEN N'Metadata Contention'
			ELSE
				N'Other'
		END ,
	AllocationPageType	=
		CASE
			WHEN WaitingTasks.PageID = 1 OR WaitingTasks.PageID % 8088 = 0
				THEN N'PFS Page'
			WHEN WaitingTasks.PageID = 2 OR WaitingTasks.PageID % 511232 = 0
				THEN N'GAM Page'
			WHEN WaitingTasks.PageID = 3 OR (WaitingTasks.PageID - 1) % 511232 = 0
				THEN N'SGAM Page'
		END ,
	MetadataSystemTable	= QUOTENAME (TempdbSchemas.[name]) + N'.' + QUOTENAME (TempdbObjects.[name])
FROM
	WaitingTasks
OUTER APPLY
	sys.dm_db_page_info (2 , FileId , PageId , 'LIMITED') AS DataPages
LEFT OUTER JOIN
	tempdb.sys.objects AS TempdbObjects
ON
	DataPages.[object_id] = TempdbObjects.[object_id]
LEFT OUTER JOIN
	tempdb.sys.schemas AS TempdbSchemas
ON
	TempdbObjects.[schema_id] = TempdbSchemas.[schema_id];
GO
