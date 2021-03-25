/*
=============================================================================
Generic script to create an indexed view and gradually migrate data into it
=============================================================================
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date: 2021-01-05
Description:
Based on Michael J. Swart's post about creating indexed views online:
https://michaeljswart.com/2015/06/how-to-create-indexed-views-online/

SQLCMD mode must be enabled to successfully execute this script.
=============================================================================
*/
SET NOEXEC OFF;
GO
-- Detect SQLCMD mode and disable script execution if SQLCMD mode is not supported.
:setvar __IsSqlCmdEnabled "True"
GO
IF N'$(__IsSqlCmdEnabled)' NOT LIKE N'True'
    BEGIN
        PRINT N'SQLCMD mode must be enabled to successfully execute this script.';
        SET NOEXEC ON;
    END
GO
:setvar DatabaseName "MyWebAppDB"

:setvar SourceTableName "dbo.ClicksLog"
:setvar SourceTableGroupByColumns "ClickPageId, ClickVisitorId, ClickStateCode"

:setvar IsMaterializedColumnName "is_materialized"
:setvar SourceTableMaterializedDefault "DF_ClicksLog_is_materialized"

:setvar IndexedViewName "dbo.Aggregated_ClicksLog_Distinct_Visitors"
:setvar IndexedViewCountBigColumnName "total_visits"
:setvar IndexedViewAdditionalAggregation ", SUM(clicks) AS total_clicks"
:setvar IndexedViewUniqueIndexName "UQ_Aggregated_ClicksLog_Distinct_Visitors"

:setvar NotMaterializedIndexOptions "WITH (ONLINE = ON, SORT_IN_TEMPDB = ON, DATA_COMPRESSION = PAGE)"

:setvar MigrationBatchSize 10000
:setvar MigrationBatchDelay "00:00:00.6"
GO
USE $(DatabaseName);
GO
-- Add a NULL materialized indicator column to the source table.
-- This should complete immediately without any data modification.

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('$(SourceTableName)') AND [name] = '$(IsMaterializedColumnName)')
	ALTER TABLE $(SourceTableName) ADD $(IsMaterializedColumnName) BIT NULL;
GO
-- Note: The following CREATE VIEW command will fail if the view already exists.
-- This should complete immediately without any data modification.
GO
CREATE VIEW $(IndexedViewName)
WITH SCHEMABINDING
AS
SELECT $(SourceTableGroupByColumns), COUNT_BIG(*) AS $(IndexedViewCountBigColumnName) $(IndexedViewAdditionalAggregation)
FROM $(SourceTableName)
WHERE $(IsMaterializedColumnName) = 1
GROUP BY $(SourceTableGroupByColumns)
GO
-- Create a unique clustered index on the view.
-- If everything is okay with the view definition, this should complete successfully
-- and immediately without any data modification.

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('$(IndexedViewName)') AND [name] = '$(IndexedViewUniqueIndexName)')
	CREATE UNIQUE CLUSTERED INDEX $(IndexedViewUniqueIndexName) ON $(IndexedViewName)
	($(SourceTableGroupByColumns));
GO
-- Once the view is indexed, add a default constraint on the source table.
-- This will cause any new data inserted from now on to automatically be "added" to the indexed view.

IF NOT EXISTS (SELECT * FROM sys.default_constraints WHERE object_id = OBJECT_ID('$(SourceTableName)') AND [name] = '$(SourceTableMaterializedDefault)')
	ALTER TABLE $(SourceTableName) ADD CONSTRAINT $(SourceTableMaterializedDefault) DEFAULT (1) FOR $(IsMaterializedColumnName);
GO

-- Create a nonclustered view on the source table for all the non-migrated data.
-- This is a heavy operation which can take a very long time.
-- But, it can be done ONLINE.
-- You should probably schedule this as part of a SQL Agent job, to run during off-peak hours.
SET QUOTED_IDENTIFIER ON;
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('$(SourceTableName)') AND [name] = 'IX_NotMaterialized')
	CREATE NONCLUSTERED INDEX IX_NotMaterialized ON $(SourceTableName) ($(SourceTableGroupByColumns)) INCLUDE($(IsMaterializedColumnName))
	WHERE $(IsMaterializedColumnName) IS NULL
	$(NotMaterializedIndexOptions);
GO

-- This will gradually migrate the data into the indexed view.
-- This can take a long time, but it's a fully ONLINE operation.
-- You should probably schedule this as part of a SQL Agent job, to run during off-peak hours.
SET QUOTED_IDENTIFIER ON;
DECLARE @SleepBetweenBatches VARCHAR(17) = '$(MigrationBatchDelay)'

SET NOCOUNT ON;

WHILE 1=1
BEGIN
	UPDATE TOP ($(MigrationBatchSize)) $(SourceTableName)
		SET $(IsMaterializedColumnName) = 1
	WHERE $(IsMaterializedColumnName) IS NULL

	IF @@ROWCOUNT = 0
		BREAK;
	
	IF NULLIF(@SleepBetweenBatches, '') IS NOT NULL
		WAITFOR DELAY @SleepBetweenBatches;
END
GO