--\
---) Author: Hans Michiels
---) Creates jobschedules in SQL Server Agent jobs that
---) were created by Ola Hallengren's maintenance solution.
--/
/*
(c) Copyright 2016 - hansmichiels.com
 
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
 
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
 
You should have received a copy of the GNU General Public License
along with this program. If not, see http://www.gnu.org/licenses/.
*/

USE [msdb]
GO

--\-------------------------------------------------------------------------------------
---) DECLARATIONS
--/-------------------------------------------------------------------------------------
DECLARE @cur_days VARCHAR(50) 
     -- @cur_days contains a space separated list of weekdays, e.g. 'mon tue wed thu fri sat sun', 
     -- or use 'all' for all days. 
     -- Please note that using 'mon tue wed thu fri sat sun' will create a weekly job with 
     -- all days checked while using 'all' will create a daily job.

DECLARE @cur_start_time INT 
     -- @cur_start_time: the starttime of the job in integer format (hhmmss)

DECLARE @cur_subday_interval INT
     -- @cur_subday_interval: N in  'every N seconds/minutes/hours (for seconds max N = 100)
     -- Use 0 when N/A

DECLARE @cur_intraday_unit VARCHAR(7)
     -- @cur_intraday_unit: seconds (s), minutes (m) or hours (h). 
     -- For configuration you can use both the full name or the first letter.

DECLARE @cur_delete_any_schedule CHAR(1)
     -- @cur_delete_any_schedule: indicates if any schedule for the given jobname should be deleted.
     -- Use NULL if you want to use the default value (recommended).
     -- It is recommended only to use 'N' if you use multiple schedules for the same job.

DECLARE @default_delete_any_schedule CHAR(1)
     -- @default_delete_any_schedule: valued used if in the configuration below
     -- [delete_any_schedule] is NULL for a specific job.

DECLARE @delete_schedules_only CHAR(1)
     -- @delete_schedules_only: if set to 'Y', only the existing schedules are deleted.
     -- No new schedules are made.
     -- If you use this setting, make sure to set @default_delete_any_schedule also to 'Y'
     -- without overruling it to 'N' for specific jobs.


DECLARE @cur_schedule_id int 
DECLARE @cur_job_name SYSNAME 
DECLARE @cur_schedule_name SYSNAME 
DECLARE @cur_freq_type INT
DECLARE @cur_freq_interval INT
DECLARE @cur_freq_subday_type INT

DECLARE @active_start_date INT
DECLARE @message VARCHAR(500)

SET @active_start_date = CONVERT(INT, CONVERT(CHAR(8), CURRENT_TIMESTAMP, 112));

--\-------------------------------------------------------------------------------------
---) CONFIGURATION
--/-------------------------------------------------------------------------------------
SET @default_delete_any_schedule = 'Y';
-- If you DO NOT want to delete all existing schedules for all jobs in your configuration, 
-- change @default_delete_any_schedule to 'N'.
-- Keeping this value on 'Y' is usually safe to do and this prevents that old schedules 
-- might in some cases not be deleted which can give trouble due to multiple (conflicting) 
-- schedules.
-- !! ONLY IF you have more than one schedule for a job, you should set [delete_any_schedule]
-- in the configuration above to 'N' for that specific job.

SET @delete_schedules_only = 'N';

DECLARE daily_weekly_cursor CURSOR LOCAL STATIC FOR  
SELECT CONVERT(SYSNAME, NULL) AS [job_name], CONVERT(SYSNAME, NULL) AS [schedule_name], 
        CONVERT(INT, NULL) AS [start_time], CONVERT(VARCHAR(50), NULL) AS [days],  
        CONVERT(INT, NULL) AS [subday_interval], 
        CONVERT(VARCHAR(7), NULL) AS [intraday_unit], 
        CONVERT(CHAR(1), NULL) AS [delete_any_schedule] 

-------------[job_name]------------------------------------[schedule]--[start]-[days]-----------------------[subday  ]-[intraday]-[delete_any]
-----------------------------------------------------------[name    ]--[time ]------------------------------[interval]-[unit    ]-[schedule  ]
UNION SELECT 'CommandLog Cleanup'                        , 'Schedule', 180000, 'wed sun'                    , 0        , NULL     , NULL
UNION SELECT 'Output File Cleanup'                       , 'Schedule', 180500, 'wed sun'                    , 0        , NULL     , NULL
UNION SELECT 'sp_delete_backuphistory'                   , 'Schedule', 181000, 'wed sun'                    , 0        , NULL     , NULL
UNION SELECT 'sp_purge_jobhistory'                       , 'Schedule', 181500, 'wed sun'                    , 0        , NULL     , NULL

UNION SELECT 'DatabaseIntegrityCheck - SYSTEM_DATABASES' , 'Schedule', 182000, 'wed sun'                    , 0        , NULL     , NULL
UNION SELECT 'DatabaseIntegrityCheck - USER_DATABASES'   , 'Schedule', 191500, 'wed sun'                    , 0        , NULL     , NULL
UNION SELECT 'DatabaseBackup - SYSTEM_DATABASES - FULL'  , 'Schedule', 001500, 'all'                        , 0        , NULL     , NULL
UNION SELECT 'DatabaseBackup - USER_DATABASES - FULL'    , 'Schedule', 003000, 'all'                        , 0        , NULL     , NULL

UNION SELECT 'IndexOptimize - USER_DATABASES'            , 'Schedule', 081000, 'mon tue wed thu fri sat sun', 0        , NULL     , NULL

/* -- Weekly full, other days diff example:
UNION SELECT 'DatabaseBackup - USER_DATABASES - FULL'    , 'Schedule', 201500, 'fri'                        , 0        , NULL     , NULL
UNION SELECT 'DatabaseBackup - USER_DATABASES - DIFF'    , 'Schedule', 201500, 'mon tue wed thu sat sun'    , 0        , NULL     , NULL
*/
/* -- Transaction log example / Example of use of [delete_any_schedule] with more than one schedule
UNION SELECT 'DatabaseBackup - USER_DATABASES - LOG'     , 'Weekdays', 000001, 'mon tue wed thu fri'        , 10       , 'minutes', 'Y'
UNION SELECT 'DatabaseBackup - USER_DATABASES - LOG'     , 'Weekend' , 000001, 'sat sun'                    , 2        , 'hours', 'N'
*/
  

--\-------------------------------------------------------------------------------------
---) CREATION OF THE SCHEDULES                                                      
--/-------------------------------------------------------------------------------------
OPEN daily_weekly_cursor   
FETCH NEXT FROM daily_weekly_cursor 
INTO @cur_job_name, @cur_schedule_name, @cur_start_time, @cur_days, 
     @cur_subday_interval, @cur_intraday_unit, @cur_delete_any_schedule

WHILE @@FETCH_STATUS = 0  
BEGIN   

    --\
    ---) Check if it is the dummy row to specify datatypes:
    --/
    IF @cur_job_name IS NULL 
    BEGIN
      -- RAISERROR('NULL', 0, 1) WITH NOWAIT;
      FETCH NEXT FROM daily_weekly_cursor 
      INTO @cur_job_name, @cur_schedule_name, @cur_start_time, @cur_days, 
           @cur_subday_interval, @cur_intraday_unit, @cur_delete_any_schedule
    END

    SET @message = 'Job: ' + @cur_job_name
    RAISERROR(@message , 0, 1) WITH NOWAIT;

    --\
    ---) Get the current schedule_id, if any.
    --/
    SELECT @cur_schedule_id = -1;
    WHILE @cur_schedule_id IS NOT NULL
    BEGIN
        SELECT @cur_schedule_id = (
            SELECT TOP 1 sch.schedule_id
            FROM msdb.dbo.sysschedules sch
              JOIN msdb.dbo.sysjobschedules js
                ON js.schedule_id = sch.schedule_id
              JOIN msdb.dbo.sysjobs jobs
                ON jobs.job_id = js.job_id
             WHERE jobs.name = @cur_job_name
               AND ( ISNULL(@cur_delete_any_schedule, @default_delete_any_schedule) = 'Y' OR sch.name = @cur_schedule_name )
            );

        --\
        ---) If schedule exists, delete it first.
        --/
        IF @cur_schedule_id IS NOT NULL 
        BEGIN
            EXEC msdb.dbo.sp_delete_schedule @schedule_id = @cur_schedule_id, @force_delete = 1;
            SET @message = '     ' + 'Schedule deleted with @schedule_id = ' + CONVERT(VARCHAR, @cur_schedule_id)
            RAISERROR(@message , 0, 1) WITH NOWAIT;
        END
    END

    --\
    ---) Set @cur_freq_type depending on days specification.
    --/
    SELECT @cur_freq_type = CASE @cur_days WHEN 'all' THEN 4 ELSE 8 END;

    --\
    ---) Set @cur_freq_interval depending on days specification.
    --/
    SELECT @cur_freq_interval = 0, @cur_days = ' ' + @cur_days + ' ';
    IF CHARINDEX(' all ', @cur_days) > 0  SET @cur_freq_interval = @cur_freq_interval + 1;
    IF CHARINDEX(' sun ', @cur_days) > 0  SET @cur_freq_interval = @cur_freq_interval + 1;
    IF CHARINDEX(' mon ', @cur_days) > 0  SET @cur_freq_interval = @cur_freq_interval + 2;
    IF CHARINDEX(' tue ', @cur_days) > 0  SET @cur_freq_interval = @cur_freq_interval + 4;
    IF CHARINDEX(' wed ', @cur_days) > 0  SET @cur_freq_interval = @cur_freq_interval + 8;
    IF CHARINDEX(' thu ', @cur_days) > 0  SET @cur_freq_interval = @cur_freq_interval + 16;
    IF CHARINDEX(' fri ', @cur_days) > 0  SET @cur_freq_interval = @cur_freq_interval + 32;
    IF CHARINDEX(' sat ', @cur_days) > 0  SET @cur_freq_interval = @cur_freq_interval + 64;
        
    IF @cur_freq_interval = 0 
    BEGIN
      RAISERROR('Days string does not contain any valid day abbreviations', 16, 1);
    END

    IF @delete_schedules_only != 'Y'
    BEGIN
        IF @cur_subday_interval = 0
        BEGIN
            EXEC msdb.dbo.sp_add_jobschedule @job_name=@cur_job_name, @name=@cur_schedule_name, 
		        @enabled=1, 
		        @freq_type=@cur_freq_type, 
		        @freq_interval=@cur_freq_interval, 
		        @freq_subday_type=1, 
		        @freq_subday_interval=0, 
		        @freq_relative_interval=0, 
		        @freq_recurrence_factor=1, 
		        @active_start_date=@active_start_date, 
		        @active_end_date=99991231, 
		        @active_start_time=@cur_start_time, 
		        @active_end_time=235959, @schedule_id = @cur_schedule_id OUTPUT;

            SET @message = '     ' + 'Schedule created for days' + @cur_days + 'at ' + CONVERT(VARCHAR, @cur_start_time)
            RAISERROR(@message , 0, 1) WITH NOWAIT;

        END ELSE BEGIN

            SELECT @cur_freq_subday_type = 
            CASE LOWER(LEFT(@cur_intraday_unit, 1)) 
            WHEN 'h' THEN 8
            WHEN 'm' THEN 4
            WHEN 's' THEN 2
            ELSE 0
            END

            IF @cur_freq_subday_type = 0
            BEGIN
              RAISERROR('''intraday_unit'' must contain a valid value when ''subday_interval'' contains a number greater than 0. Please check your configuration.', 16, 1);
            END

            EXEC msdb.dbo.sp_add_jobschedule @job_name=@cur_job_name, @name=@cur_schedule_name, 
		        @enabled=1, 
		        @freq_type=@cur_freq_type, 
		        @freq_interval=@cur_freq_interval, 
		        @freq_subday_type=@cur_freq_subday_type,
		        @freq_subday_interval=@cur_subday_interval, 
		        @freq_relative_interval=0, 
		        @freq_recurrence_factor=1, 
		        @active_start_date=@active_start_date, 
		        @active_end_date=99991231, 
		        @active_start_time=@cur_start_time, 
		        @active_end_time=235959, @schedule_id = @cur_schedule_id OUTPUT;

            SET @message = '     ' + 'Schedule created for days' + @cur_days + 'every ' + CONVERT(VARCHAR, @cur_subday_interval) + ' ' + @cur_intraday_unit
            RAISERROR(@message , 0, 1) WITH NOWAIT;

        END
    END
    RAISERROR('-----' , 0, 1) WITH NOWAIT;
    
    SET @cur_schedule_id = NULL;
    FETCH NEXT FROM daily_weekly_cursor 
    INTO @cur_job_name, @cur_schedule_name, @cur_start_time, @cur_days, 
         @cur_subday_interval, @cur_intraday_unit, @cur_delete_any_schedule
END   

CLOSE daily_weekly_cursor   
DEALLOCATE daily_weekly_cursor 

