USE [DBA]
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_DBA_StartCDCCaptureJobs]
AS

/*********************************************************************************************************
Author:			Reut Almog Talmi @Madeira Data Solutions
Created Date:	2020-04-04
Description:	Procedure at a manager database level that handles start CDC capture jobs 
				in all databases which are enabled for CDC and are also involved in mirroring
**********************************************************************************************************/


SET NOCOUNT ON

DROP TABLE IF EXISTS #CurrentRunningJobs;
CREATE TABLE #CurrentRunningJobs (JobName SYSNAME)


INSERT INTO #CurrentRunningJobs 
SELECT 
	j.name AS JobName 
FROM 
	msdb.dbo.sysjobs j
INNER JOIN 
	msdb.dbo.sysjobactivity ja ON j.job_id = ja.job_id
INNER JOIN 
	msdb.dbo.syssessions sess ON sess.session_id = ja.session_id
INNER JOIN 
	(
	SELECT 
		MAX(agent_start_date) AS max_agent_start_date 
	FROM 
		msdb.dbo.syssessions) sess_max ON sess.agent_start_date = sess_max.max_agent_start_date
WHERE 
    ja.run_requested_date IS NOT NULL 
AND 
	ja.stop_execution_date IS NULL			




DECLARE 
	@SQLCommand NVARCHAR(4000),
	@DBName SYSNAME,
	@JobName SYSNAME


DECLARE DBCursor CURSOR READ_ONLY FORWARD_ONLY FOR 
	SELECT 
		d.name 
	FROM 
		sys.databases d
	INNER JOIN 
		sys.database_mirroring dm ON d.database_id = dm.database_id
	WHERE 
		d.is_cdc_enabled = 1
	AND 
		dm.mirroring_role = 1		--PRINCIPAL		


OPEN DBCursor 
FETCH NEXT FROM DBCursor INTO @DBName

WHILE @@FETCH_STATUS = 0
	BEGIN

		SELECT @JobName = N'cdc.'+ @DBName + '_capture'

		IF NOT EXISTS (SELECT 1 FROM #CurrentRunningJobs WHERE JobName = @JobName)		
		BEGIN

			SELECT @SQLCommand = N'USE ' + QUOTENAME(@DBName) + ';' + CHAR(10)+
			N'EXEC sys.sp_cdc_start_job @job_type = N''capture'';' + CHAR(10) + CHAR(13)  
			PRINT @SQLCommand 
			EXEC sp_executesql @SQLCommand
		END
		ELSE
		BEGIN
			PRINT N'Job '+ @JobName +' is already running'
		END	
		FETCH NEXT FROM DBCursor INTO @DBName

	END

CLOSE DBCursor
DEALLOCATE DBCursor



GO




------------------------------------------------------------------------------------------------------------------------------


USE [DBA]
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_DBA_StopCDCCaptureJobs]
AS


/*********************************************************************************************************
Author:			Reut Almog Talmi @Madeira Data Solutions
Created Date:	2020-04-04
Description:	Procedure at a manager database level that handles stop CDC capture jobs 
				in all databases which are enabled for CDC and are also involved in mirroring
**********************************************************************************************************/

SET NOCOUNT ON


DROP TABLE IF EXISTS #CurrentRunningJobs;

CREATE TABLE #CurrentRunningJobs (JobName SYSNAME)


INSERT INTO #CurrentRunningJobs 
SELECT 
	j.name AS JobName 
FROM 
	msdb.dbo.sysjobs j
INNER JOIN 
	msdb.dbo.sysjobactivity ja ON j.job_id = ja.job_id
INNER JOIN 
	msdb.dbo.syssessions sess ON sess.session_id = ja.session_id
INNER JOIN 
	(
	SELECT 
		MAX(agent_start_date) AS max_agent_start_date 
	FROM 
		msdb.dbo.syssessions) sess_max ON sess.agent_start_date = sess_max.max_agent_start_date
WHERE 
    ja.run_requested_date IS NOT NULL 
AND 
	ja.stop_execution_date IS NULL



DECLARE 
	@SQLCommand NVARCHAR(4000),
	@DBName SYSNAME,
	@JobName SYSNAME


DECLARE DBCursor CURSOR READ_ONLY FORWARD_ONLY FOR 
	SELECT 
		d.name 
	FROM 
		sys.databases d
	INNER JOIN 
		sys.database_mirroring dm ON d.database_id = dm.database_id
	WHERE 
		d.is_cdc_enabled = 1
	AND 
		dm.mirroring_role = 1		--PRINCIPAL


OPEN DBCursor 
FETCH NEXT FROM DBCursor INTO @DBName

WHILE @@FETCH_STATUS = 0
	BEGIN
		
		
		SELECT @JobName = N'cdc.'+ @DBName + '_capture'

		IF EXISTS (SELECT 1 FROM #CurrentRunningJobs WHERE JobName = @JobName)
		BEGIN
			
			SELECT @SQLCommand = N'USE '+ QUOTENAME(@DBName) + '; EXEC sys.sp_cdc_stop_job @job_type = N''capture'';' + CHAR(10) + CHAR(13)
		  	PRINT @SQLCommand 
			EXEC sp_executesql @SQLCommand
		END
		ELSE
		BEGIN
			PRINT N'Job '+ @JobName +' is not currently running'
		END
		
		FETCH NEXT FROM DBCursor INTO @DBName

	END

CLOSE DBCursor
DEALLOCATE DBCursor


GO






