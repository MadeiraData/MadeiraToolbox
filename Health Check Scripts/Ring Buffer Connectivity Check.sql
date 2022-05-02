SELECT 
	record.value('(Record/@id)[1]', 'int') as id,
	record.value('(Record/@type)[1]', 'varchar(50)') as type,
	record.value('(Record/ConnectivityTraceRecord/RecordType)[1]', 'varchar(50)') as RecordType,
	record.value('(Record/ConnectivityTraceRecord/RecordSource)[1]', 'varchar(50)') as RecordSource,
	record.value('(Record/ConnectivityTraceRecord/Spid)[1]', 'int') as Spid,
	record.value('(Record/ConnectivityTraceRecord/SniConnectionId)[1]', 'uniqueidentifier') as SniConnectionId,
	record.value('(Record/ConnectivityTraceRecord/SniProvider)[1]', 'int') as SniProvider,
	record.value('(Record/ConnectivityTraceRecord/OSError)[1]', 'int') as OSError,
	record.value('(Record/ConnectivityTraceRecord/SniConsumerError)[1]', 'int') as SniConsumerError,
	record.value('(Record/ConnectivityTraceRecord/State)[1]', 'int') as State,
	record.value('(Record/ConnectivityTraceRecord/RemoteHost)[1]', 'varchar(50)') as RemoteHost,
	record.value('(Record/ConnectivityTraceRecord/RemotePort)[1]', 'varchar(50)') as RemotePort,
	record.value('(Record/ConnectivityTraceRecord/LocalHost)[1]', 'varchar(50)') as LocalHost,
	record.value('(Record/ConnectivityTraceRecord/LocalPort)[1]', 'varchar(50)') as LocalPort,
	record.value('(Record/ConnectivityTraceRecord/RecordTime)[1]', 'datetime') as RecordTime,
	record.value('(Record/ConnectivityTraceRecord/LoginTimers/TotalLoginTimeInMilliseconds)[1]', 'bigint') as TotalLoginTimeInMilliseconds,
	record.value('(Record/ConnectivityTraceRecord/LoginTimers/LoginTaskEnqueuedInMilliseconds)[1]', 'bigint') as LoginTaskEnqueuedInMilliseconds,
	record.value('(Record/ConnectivityTraceRecord/LoginTimers/NetworkWritesInMilliseconds)[1]', 'bigint') as NetworkWritesInMilliseconds,
	record.value('(Record/ConnectivityTraceRecord/LoginTimers/NetworkReadsInMilliseconds)[1]', 'bigint') as NetworkReadsInMilliseconds,
	record.value('(Record/ConnectivityTraceRecord/LoginTimers/SslProcessingInMilliseconds)[1]', 'bigint') as SslProcessingInMilliseconds,
	record.value('(Record/ConnectivityTraceRecord/LoginTimers/SspiProcessingInMilliseconds)[1]', 'bigint') as SspiProcessingInMilliseconds,
	record.value('(Record/ConnectivityTraceRecord/LoginTimers/LoginTriggerAndResourceGovernorProcessingInMilliseconds)[1]', 'bigint') as LoginTriggerAndResourceGovernorProcessingInMilliseconds,
	record.value('(Record/ConnectivityTraceRecord/TdsBuffersInformation/TdsInputBufferError)[1]', 'int') as TdsInputBufferError,
	record.value('(Record/ConnectivityTraceRecord/TdsBuffersInformation/TdsOutputBufferError)[1]', 'int') as TdsOutputBufferError,
	record.value('(Record/ConnectivityTraceRecord/TdsBuffersInformation/TdsInputBufferBytes)[1]', 'int') as TdsInputBufferBytes,
	record.value('(Record/ConnectivityTraceRecord/TdsDisconnectFlags/PhysicalConnectionIsKilled)[1]', 'int') as PhysicalConnectionIsKilled,
	record.value('(Record/ConnectivityTraceRecord/TdsDisconnectFlags/DisconnectDueToReadError)[1]', 'int') as DisconnectDueToReadError,
	record.value('(Record/ConnectivityTraceRecord/TdsDisconnectFlags/NetworkErrorFoundInInputStream)[1]', 'int') as NetworkErrorFoundInInputStream,
	record.value('(Record/ConnectivityTraceRecord/TdsDisconnectFlags/ErrorFoundBeforeLogin)[1]', 'int') as ErrorFoundBeforeLogin,
	record.value('(Record/ConnectivityTraceRecord/TdsDisconnectFlags/SessionIsKilled)[1]', 'int') as SessionIsKilled,
	record.value('(Record/ConnectivityTraceRecord/TdsDisconnectFlags/NormalDisconnect)[1]', 'int') as NormalDisconnect,
	record.value('(Record/ConnectivityTraceRecord/TdsDisconnectFlags/NormalLogout)[1]', 'varchar(3)') as NormalLogout,
	record.query('.') AS xmlData
FROM
(
	SELECT CAST(record as xml) as record
	FROM sys.dm_os_ring_buffers
	WHERE ring_buffer_type = 'RING_BUFFER_CONNECTIVITY'
) as tab
--WHERE record.value('(Record/ConnectivityTraceRecord/RecordType)[1]', 'varchar(50)') IN ('Error','ConnectionClose') --Comment Out to see all RecordTypes
ORDER BY RecordTime DESC
