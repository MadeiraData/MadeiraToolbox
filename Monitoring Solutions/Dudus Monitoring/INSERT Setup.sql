--IF SCHEMA_ID('Report') IS NULL 
--	BEGIN 
--		DECLARE @Command NVARCHAR(MAX) = N'CREATE SCHEMA [Report]';
--		EXEC (@Command);
--	END;

--GO

--CREATE TABLE [Report].[MonitoringAlerts]
--(
--	Id INT IDENTITY constraint [PK_Report_MonitoringAlerts] PRIMARY KEY,
--	Name NVARCHAR(1000) NOT NULL,
--	ExecutionCommand NVARCHAR(MAX) NOT NULL,
--	Enabled BIT NOT NULL
--)

/*
DECLARE @HTMLTable NVARCHAR(MAX)

EXEC [Report].[usp_NonWorkingServices]
	@DebugMode = 0,
	@HTMLTable = @HTMLTable OUTPUT

SELECT @HTMLTable


*/


SET IDENTITY_INSERT [Report].[MonitoringAlerts]  ON 
INSERT INTO [Report].[MonitoringAlerts] 
(	[Id],
	[Name], 
	[ExecutionCommand], 
	[Enabled]
)
VALUES 
(
8,
N'Non Working Services',
N'
EXEC [Report].[usp_NonWorkingServices]
	@DebugMode = 0,
	@HTMLTable = @HTMLTable OUTPUT
',
1
)
SET IDENTITY_INSERT [Report].[MonitoringAlerts]  OFF 



--DELETE [Report].[MonitoringAlerts]  WHERE id = 1 