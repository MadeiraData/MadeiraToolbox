/*========================================================================================================================

Description:	Display information about SQL Server Agent jobs
Scope:			Instance
Author:			Guy Glantser
Created:		11/09/2020
Last Updated:	11/09/2020
Notes:			Use this information to consider existing job schedules when planning a maintenance plan

=========================================================================================================================*/

USE
	msdb;
GO


SELECT
	JobName								= Jobs.[name] ,
	JobDescription						= Jobs.[description] ,
	JobCategory							= JobCategories.[name] ,
	ScheduleName						= Schedules.[name] ,
	SchduleFrequency					=
		CASE
			WHEN Schedules.freq_type = 1
				THEN N'One-Time'
			WHEN Schedules.freq_type = 4
				THEN N'Daily'
			WHEN Schedules.freq_type = 8
				THEN N'Weekly'
			WHEN Schedules.freq_type = 16
				THEN N'Monthly'
			WHEN Schedules.freq_type = 32
				THEN N'Monthly-Relative'
			WHEN Schedules.freq_type = 64
				THEN N'Agent Startup'
			WHEN Schedules.freq_type = 128
				THEN N'Computer Idle'
		END ,
	ScheduleFrequencyInterval			= Schedules.freq_interval ,
	ScheduleFrequencySubdayType			=
		CASE
			WHEN Schedules.freq_subday_type = 0
				THEN N'Unused'
			WHEN Schedules.freq_subday_type = 1
				THEN N'At Time'
			WHEN Schedules.freq_subday_type = 2
				THEN N'Seconds'
			WHEN Schedules.freq_subday_type = 4
				THEN N'Minutes'
			WHEN Schedules.freq_subday_type = 8
				THEN N'Hours'
		END ,
	ScheduleFrequencySubdayInterval		= Schedules.freq_subday_interval ,
	ScheduleFrequencyRelativeInterval	=
		CASE
			WHEN Schedules.freq_relative_interval = 0
				THEN N'Unused'
			WHEN Schedules.freq_relative_interval = 1
				THEN N'First'
			WHEN Schedules.freq_relative_interval = 2
				THEN N'Second'
			WHEN Schedules.freq_relative_interval = 4
				THEN N'Third'
			WHEN Schedules.freq_relative_interval = 8
				THEN N'Fourth'
			WHEN Schedules.freq_relative_interval = 16
				THEN N'Last'
		END ,
	ScheduleFrequencyRecurrenceFactor	= Schedules.freq_recurrence_factor ,
	ScheduleActiveStartDateTime			= CAST (dbo.agent_datetime (Schedules.active_start_date , Schedules.active_start_time) AS DATETIME2(0)) ,
	ScheduleActiveEndDateTime			= CAST (dbo.agent_datetime (Schedules.active_end_date , Schedules.active_end_time) AS DATETIME2(0)) ,
	AverageDuration_Seconds				= AVG (JobHistory.run_duration % 100 + JobHistory.run_duration / 100 % 100 * 60 + JobHistory.run_duration / 10000 * 3600) ,
	HasFailedInThePast					=
		CASE
			WHEN
				SUM
				(
					CASE
						WHEN JobHistory.run_status = 0
							THEN 1
						ELSE
							0
					END
				)
				> 0
			THEN
				CAST (1 AS BIT)
			ELSE
				CAST (0 AS BIT)
		END ,
	LastRunStartDateTime				= MAX (CAST (dbo.agent_datetime (JobHistory.run_date , JobHistory.run_time) AS DATETIME2(0))) ,
	NextRunStartDateTime				= CAST (dbo.agent_datetime (JobSchedules.next_run_date , JobSchedules.next_run_time) AS DATETIME2(0))
FROM
	dbo.sysjobs AS Jobs
INNER JOIN
	dbo.syscategories AS JobCategories
ON
	Jobs.category_id = JobCategories.category_id
LEFT OUTER JOIN
(
	dbo.sysjobschedules AS JobSchedules
INNER JOIN
	dbo.sysschedules AS Schedules
ON
	JobSchedules.schedule_id = Schedules.schedule_id
AND
	Schedules.[enabled] = 1
)
ON
	Jobs.job_id = JobSchedules.job_id
LEFT OUTER JOIN
	dbo.sysjobhistory AS JobHistory
ON
	Jobs.job_id = JobHistory.job_id
AND
	JobHistory.step_id = 0	-- Job Outcome
WHERE
	Jobs.[enabled] = 1
GROUP BY
	Jobs.job_id ,
	Jobs.[name] ,
	Jobs.[description] ,
	JobCategories.[name] ,
	Schedules.schedule_id ,
	Schedules.[name] ,
	Schedules.freq_type ,
	Schedules.freq_interval ,
	Schedules.freq_subday_type ,
	Schedules.freq_subday_interval ,
	Schedules.freq_relative_interval ,
	Schedules.freq_recurrence_factor ,
	Schedules.active_start_date ,
	Schedules.active_start_time ,
	Schedules.active_end_date ,
	Schedules.active_end_time ,
	JobSchedules.next_run_date  ,
	JobSchedules.next_run_time
ORDER BY
	Jobs.job_id				ASC ,
	Schedules.schedule_id	ASC;
GO
