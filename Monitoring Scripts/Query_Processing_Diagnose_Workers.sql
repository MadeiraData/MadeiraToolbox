SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

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