USE [YourDBname]
GO

CREATE TRIGGER [DDLEvents_SampleTrigger]
    ON DATABASE -- ALL SERVER
    FOR DDL_DATABASE_LEVEL_EVENTS -- DDL_SERVER_LEVEL_EVENTS
AS
BEGIN
 
	SET NOCOUNT ON;
	
	DECLARE
        @FullData	XML			= EVENTDATA(),
        @IPAddress	VARCHAR(32)	=
									(
										SELECT TOP (1)
											REPLACE(client_net_address, '<local machine>', '127.0.0.1')
										FROM
											sys.dm_exec_connections
										WHERE
											session_id = @@SPID
									);


    INSERT [YourSchemaName].[DDLEventsAudit]
    (
        [Type],
        TSQLCommand,
        [Database],
        [Schema],
        [Object],
		[ByLogin],
        Program,		
        FromHost,
        IPAddress,
		EventXML
    )
    SELECT
        @FullData.value('(/EVENT_INSTANCE/EventType)[1]',   'NVARCHAR(128)'), 
        @FullData.value('(/EVENT_INSTANCE/TSQLCommand)[1]', 'NVARCHAR(MAX)'),
        DB_NAME(),
        @FullData.value('(/EVENT_INSTANCE/SchemaName)[1]',  'NVARCHAR(255)'), 
        @FullData.value('(/EVENT_INSTANCE/ObjectName)[1]',  'NVARCHAR(255)'),
        SUSER_SNAME(),		
        PROGRAM_NAME(),
        HOST_NAME(),
        @IPAddress,
		@FullData;

END