/*
Expanding CommandLog with auditing columns
==========================================
Author: Eitan Blumin | https://madeiradata.com
Date: 2024-02-14
Description:
Use this script to add several auditing columns to
the CommandLog table for Ola Hallengren's Maintenance Solution.
These columns can be used to identify the processes that perform
maintenance operations using Ola's framework.
*/
GO
CREATE OR ALTER FUNCTION dbo.GetCurrentInputBuffer()
RETURNS nvarchar(max)
AS
BEGIN
      RETURN (SELECT TOP(1) event_info FROM sys.dm_exec_input_buffer(@@SPID,NULL))
END
GO
ALTER TABLE [dbo].[CommandLog] ADD
      [Hostname] [sysname] COLLATE SQL_Latin1_General_CP1_CI_AS NULL CONSTRAINT DF_CommandLog_Hostname DEFAULT(HOST_NAME()),
      [HostPID] [int] NULL DEFAULT(HOST_ID()),
      [AppName] [sysname] COLLATE SQL_Latin1_General_CP1_CI_AS NULL CONSTRAINT DF_CommandLog_AppName DEFAULT(APP_NAME()),
      [LoginName] [sysname] COLLATE SQL_Latin1_General_CP1_CI_AS NULL CONSTRAINT DF_CommandLog_LoginName DEFAULT(SUSER_SNAME()),
      [SPID] [int] NULL CONSTRAINT DF_CommandLog_SPID DEFAULT(@@SPID),
      [InputBuffer] nvarchar(max) CONSTRAINT DF_CommandLog_InputBuffer DEFAULT(dbo.GetCurrentInputBuffer()),
      [IPAddress] [sysname] COLLATE SQL_Latin1_General_CP1_CI_AS NULL CONSTRAINT DF_CommandLog_IPAddress DEFAULT(CONVERT(sysname, CONNECTIONPROPERTY('client_net_address')))
GO
