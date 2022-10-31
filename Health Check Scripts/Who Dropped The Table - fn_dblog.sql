/*
Source: https://royalsql.com/2022/10/25/transactions-follow-me-left-and-right-but-who-did-that-over-here/
*/
SELECT [Current LSN]
		,[Operation]
		,[Context]
		,[Transaction ID]
		,[Description]
		,[Begin Time]
		,[Transaction SID]
		,SUSER_SNAME ([Transaction SID]) AS WhoDidIt
FROM sys.fn_dblog (NULL,NULL)
INNER JOIN(SELECT [Transaction ID] AS tid
FROM sys.fn_dblog(NULL,NULL)
WHERE [Transaction Name] LIKE 'DROPOBJ%')fd ON [Transaction ID] = fd.tid