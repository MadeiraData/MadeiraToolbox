USE [msdb]
GO

/****** Object:  Alert [DB Mirroring: State Changes]    Script Date: 03/11/2014 14:55:46 ******/
EXEC msdb.dbo.sp_add_alert @name=N'DB Mirroring: State Changes', 
		@message_id=0, 
		@severity=0, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=1, 
		@category_name=N'Database Mirroring', 
		@wmi_namespace=N'\\.\root\Microsoft\SqlServer\ServerEvents\MSSQLSERVER', 
		@wmi_query=N'SELECT * from DATABASE_MIRRORING_STATE_CHANGE WHERE (State = 8 OR State = 7)', 
		@job_id=N'1124e724-6b45-4860-919c-5a0f2d061b9b'
GO


