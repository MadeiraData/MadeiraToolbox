SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @MinPendingTasksForAlert INT;
DECLARE @MinCreatedWorkersPercent INT;
SET @MinPendingTasksForAlert = 25;
SET @MinCreatedWorkersPercent = 85;

DECLARE @results AS TABLE
(
 id INT IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
 msg NVARCHAR(MAX) COLLATE database_default NULL
);

INSERT INTO @results(msg)
select N'In server: ' + @@SERVERNAME + N', high number of pending worker tasks detected: '
+ CONVERT(nvarchar(max), COUNT(*))
from sys.dm_os_tasks
WHERE task_state = 'PENDING'
HAVING COUNT(*) >= @MinPendingTasksForAlert;

IF @@ROWCOUNT > 0 AND OBJECT_ID('sp_server_diagnostics') IS NOT NULL
BEGIN
 IF OBJECT_ID('tempdb..#diagnostics') IS NOT NULL DROP TABLE #diagnostics;
 CREATE TABLE #diagnostics
 (
  create_time DATETIME NULL,
  component_type nvarchar(255) COLLATE database_default NULL,
  component_name SYSNAME COLLATE database_default NULL,
  [state] INT NULL,
  state_desc SYSNAME COLLATE database_default NULL,
  event_data_xml XML NULL
 );
 INSERT INTO #diagnostics
 EXEC sp_server_diagnostics

 INSERT INTO @results(msg)
 SELECT m.msg
 FROM
 (
  SELECT
    create_time, state_desc
  , maxWorkers  = event_data_xml.value('(queryProcessing/@maxWorkers)[1]', 'int')
  , workersCreated = event_data_xml.value('(queryProcessing/@workersCreated)[1]', 'int')
  , workersIdle  = event_data_xml.value('(queryProcessing/@workersIdle)[1]', 'int')
  , pendingTasks  = event_data_xml.value('(queryProcessing/@pendingTasks)[1]', 'int')
  , blockedProcesses = event_data_xml.value('count(*//blocked-process-report/blocked-process/process/inputbuf)', 'int')
  , blockingProcesses = event_data_xml.value('count(*//blocked-process-report/blocking-process/process/inputbuf)', 'int')
  , blockedByNonSession = event_data_xml.value('count(*//blocked-process-report/blocked-process/process[empty(../../blocking-process/process/inputbuf/text())]/../..)', 'int')
  , possibleHeadBlockers = event_data_xml.query('
  let $items := distinct-values(*//blocked-process-report/blocking-process/process/@spid)
  return
   <blockers totalCount="{count($items)}">
   {
      for $spid in $items
       let $blockedByResource := *//blocked-process-report/blocked-process/process[@spid = $spid and empty(../../blocking-process/process/inputbuf/text())]/../..
      let $isBlockedByResource := not(empty(*//blocked-process-report/blocked-process/process[@spid = $spid]))
       return
       <blocker spid="{$spid}" is-blocked-by-non-session="{$isBlockedByResource}">
       {$blockedByResource}
       </blocker>
   }
   </blockers>').query('let $items := *//blocker[not(@is-blocked-by-non-session) or not(empty(*//process/inputbuf/text()))]
  return
   <head-blockers totalCount="{count($items)}">{$items}</head-blockers>')
  FROM #diagnostics
  WHERE component_name = 'query_processing'
 ) AS d
 CROSS APPLY
 (
  SELECT N'system state as of ' + CONVERT(nvarchar(25), create_time, 121) + N': ' + QUOTENAME(state_desc) 
  + N'. maxWorkers: ' + CONVERT(nvarchar(max), maxWorkers)
  + N', workersCreated: ' + CONVERT(nvarchar(max), workersCreated)
  + N', workersIdle: ' + CONVERT(nvarchar(max), workersIdle)
  + N', pendingTasks: ' + CONVERT(nvarchar(max), pendingTasks)

  UNION ALL

  SELECT N'blocked processes: ' + CONVERT(nvarchar(max), blockedProcesses)
  + N', processes blocked by non-session: ' + CONVERT(nvarchar(max), blockedByNonSession)
  + N', blocking processes: ' + CONVERT(nvarchar(max), blockingProcesses)
  + N', possible head blockers: ' + CONVERT(nvarchar(max), possibleHeadBlockers.value('(head-blockers/@totalCount)[1]','int'))

  UNION ALL

  SELECT DISTINCT N'Possible head blocker SPID: ' + x.query('.').value('(process/@spid)[1]', 'nvarchar(max)')
  FROM possibleHeadBlockers.nodes('*//process') AS n(x)

  UNION ALL

  SELECT DISTINCT x.query('.').value('(inputbuf)[1]', 'nvarchar(max)')
  FROM possibleHeadBlockers.nodes('*//inputbuf') AS n(x)
 ) AS m(msg)
 WHERE m.msg IS NOT NULL
 AND pendingTasks >= @MinPendingTasksForAlert
 AND workersCreated * 100.0 / maxWorkers >= @MinCreatedWorkersPercent
 ;

 -- If nothing returned from diagnostics, then this was a false positive
 IF @@ROWCOUNT = 0
  DELETE FROM @results;
END

SELECT msg, id
FROM @results
