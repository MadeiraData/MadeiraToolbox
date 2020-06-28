USE db_TestCDC
GO

CREATE OR ALTER PROCEDURE dbo.sp_DBA_AddColumnToCDCTable 

/*********************************************************************************************************
Author:			Reut Almog Talmi @Madeira
Created Date:	2020-06-04
Description:	Procedure at database level that handles adding column/s into CDC captured tables 
				*in the future, other actions will be added such as DROP

How to Execute: sp_DBA_AddColumnToCDCTable 'dbo','T','ColumnA, INT, NULL ; ColumnB, NVARCHAR(100), NOT NULL','ADD'
				@pColumnList parameter should contain all columns and their definitions. 
				use ';' delimiter between different columns
				and ',' delimiter between column definitions in the following convention: 
				'ColumnA, INT, NULL ; ColumnB , NVARCHAR(100) , NOT NULL'
				spaces anywhere in this parameter will not harm the code				

Based on procedure from http://www.techbrothersit.com/2013/06/change-data-capture-cdc-sql-server-add.html
**********************************************************************************************************/


@pSchemaName		SYSNAME,
@pTableName			SYSNAME,
@pColumnList		NVARCHAR(MAX),				
@pAction			NVARCHAR(10) = 'ADD'		

AS

SET NOCOUNT ON;

BEGIN

	DECLARE
		@vTempTableName			SYSNAME,
		@vCaptureInstance		SYSNAME,
		@vCDCTableName			SYSNAME,
		@vColumnExists			BIT,
		@vColumnExistsCDC		BIT,
		@vSQLLockTable			NVARCHAR(MAX),
		@vSQLTempTable			NVARCHAR(MAX),
		@vSQLAlterTable			NVARCHAR(MAX),
		@vSQLAddClause			NVARCHAR(MAX),
		@vSQLAlterTempTable		NVARCHAR(MAX),
		@vDDLTriggerExists		BIT,
		@vDDLTriggerName		SYSNAME,
		@vSQLDisableDDLTrigger	NVARCHAR(1000),
		@vSQLEnableDDLTrigger	NVARCHAR(1000),
		@vSQLDisableCDC			NVARCHAR(MAX),
		@vSQLEnableCDC			NVARCHAR(MAX),
		@vSQLInsertColumnList	NVARCHAR(MAX) ,
		@vSQLDropTempTable		NVARCHAR(MAX)

	
	-- Get CDC Table Name and Capture instance name
	DROP TABLE IF EXISTS #TableMetaData

	SELECT OBJECT_NAME(source_object_id) AS TableName, 
	capture_instance AS CaptureInstance,
	OBJECT_NAME(object_id) AS CDCTableName
	INTO #TableMetaData
	FROM cdc.change_tables
	WHERE source_object_id = OBJECT_ID(@pTableName)


	SET @vTempTableName = (SELECT CONCAT(CDCTableName, '_Temp') FROM #TableMetaData)
	SET @vCaptureInstance = (SELECT CaptureInstance FROM #TableMetaData)
	SET @vCDCTableName = (SELECT CDCTableName FROM #TableMetaData)

	
	-- Set Up Columns Definition and related variables
	SET @pColumnList = REPLACE (REPLACE(@pColumnList, ' ', ''), 'NOTNULL', 'NOT NULL')
	

	DROP TABLE IF EXISTS #ColumnsDefinition

	SELECT ColumnNo, Value AS ColumnDefinition 
	INTO #ColumnsDefinition
	FROM
		(
			SELECT 
				ROW_NUMBER() OVER (ORDER BY (SELECT 1) ASC) AS ColumnNo, 
				Value AS ColumnDefinition
			FROM STRING_SPLIT(@pColumnList, ';')
		) AS T
	CROSS APPLY STRING_SPLIT(ColumnDefinition, ',')

	--SELECT * FROM #ColumnsDefinition
	
		
	DROP TABLE IF EXISTS #AddColumns
	
	SELECT 
	ROW_NUMBER() OVER (ORDER BY (SELECT 1) ASC) AS ColumnNo, 
	REPLACE(Value, ',', ' ') AS ColumnDefinition
	INTO #AddColumns
	FROM STRING_SPLIT(RTRIM(LTRIM(@pColumnList)), ';')
	 
	--SELECT * FROM #AddColumns


	SET @vSQLAddClause = N' ADD ' + (SELECT STRING_AGG(ColumnDefinition, ',') FROM #AddColumns) + ';'


	
	--stop capture job
	EXEC sys.sp_cdc_stop_job @job_type = N'capture';


		BEGIN TRY
			
			IF @pAction = 'ADD'

            BEGIN
                --Check if Column exists for SourceTable table
				IF EXISTS	(	
							SELECT 1 
							FROM INFORMATION_SCHEMA.COLUMNS 
							WHERE  TABLE_SCHEMA = @pSchemaName 
							AND TABLE_NAME = @pTableName 
							AND COLUMN_NAME IN	(
												SELECT SUBSTRING(ColumnDefinition, 1 , (CHARINDEX(' ',ColumnDefinition))) 
												FROM #AddColumns
												)
							)
				BEGIN
					SET @vColumnExists = 1
					PRINT N'-- column/s already exists in Source Table ::' + @pTableName + '-->Proceeding to Next Step.'
				END
				ELSE
				BEGIN
					SET @vColumnExists = 0
					PRINT N'-- column/s does not exists in Source Table ::' + @pTableName + + N' -->Proceeding to ADD these column'
				END

				
                --Check if Column exists for CDC table
				IF EXISTS	(
							SELECT 1 
							FROM INFORMATION_SCHEMA.COLUMNS 
							WHERE  TABLE_SCHEMA = 'cdc' 
							AND TABLE_NAME = @vCDCTableName 
							AND COLUMN_NAME IN	(
												SELECT SUBSTRING(ColumnDefinition, 1 , (CHARINDEX(' ',ColumnDefinition))) 
												FROM #AddColumns
												)
							)
				BEGIN
					SET @vColumnExistsCDC = 1
					PRINT N'-- column/s are already part of CDC Tble ::cdc.'+ @vCDCTableName
				END
				ELSE
				BEGIN
					SET @vColumnExistsCDC = 0
					PRINT N'-- column/s are not part of CDC Tble ::cdc.' + @vCDCTableName + N' -->Proceeding to ADD these column'
				END


				BEGIN TRANSACTION AddColumnsToCDCTable

				--1. Lock the table for DML changes
				SET @vSQLLockTable = N'SELECT TOP 1 * FROM ' + @pTableName + N' WITH (TABLOCK, HOLDLOCK)' + CHAR(10) + CHAR(13)

				PRINT @vSQLLockTable
				EXEC sp_executesql @vSQLLockTable

                --2. Copy existing CDC Table Into temp table
				SET @vSQLTempTable=	N'DROP TABLE IF EXISTS ' + @vTempTableName + ';'	+ CHAR(10) + CHAR(13) + 
									N'SELECT * INTO ' + @vTempTableName					+ CHAR(10) +  
									N'FROM cdc.' + @vCDCTableName + ';'					+ CHAR(10) + CHAR(13)

				PRINT @vSQLTempTable
				EXEC sp_executesql @vSQLTempTable


				IF @vColumnExists = 0
				BEGIN
					--3.a. Add the desired column/s to base table
					SET @vSQLAlterTable =	N'ALTER TABLE ' + @pTableName + CHAR(10) + @vSQLAddClause + CHAR(10) + CHAR(13)

					PRINT @vSQLAlterTable
					EXEC sp_executesql @vSQLAlterTable
				END

				IF @vColumnExistsCDC = 0
				BEGIN
					--3.b. Add the desired column/s to temp table
					SET @vSQLAlterTempTable = N'ALTER TABLE ' + @vTempTableName + CHAR(10) + @vSQLAddClause + CHAR(10) + CHAR(13)

					PRINT @vSQLAlterTempTable
					EXEC sp_executesql @vSQLAlterTempTable
				END

				
				-- Disable DDL Trigger if exists - required before disabling/enabling CDC on Table
				IF EXISTS (SELECT 1 FROM sys.triggers WHERE parent_class = 0 AND is_disabled = 0 AND name != 'tr_MScdc_ddl_event')
				BEGIN
					SET @vDDLTriggerExists = 1
					SET @vDDLTriggerName = (SELECT name FROM sys.triggers WHERE parent_class = 0 AND is_disabled = 0 AND name != 'tr_MScdc_ddl_event')
				END

				IF @vDDLTriggerExists = 1
				BEGIN
					SET @vSQLDisableDDLTrigger = N'DISABLE TRIGGER '+QUOTENAME(@vDDLTriggerName)+' ON DATABASE;' + CHAR(10) + CHAR(13)
											
					PRINT @vSQLDisableDDLTrigger 
					EXEC sp_executesql @vSQLDisableDDLTrigger 
				END


				--4. Disable CDC on the source table, this will drop the associated CDC table
				SET @vSQLDisableCDC = N'EXEC sys.sp_cdc_disable_table ' + CHAR(10) +
				N'@source_schema = ''' + @pSchemaName + ''',' + CHAR(10) +
				N'@source_name = ''' + @pTableName + ''',' + CHAR(10) +
				N'@capture_instance = ''' + @vCaptureInstance + ''';' + CHAR(10) + CHAR(13)
				
				PRINT @vSQLDisableCDC 
				EXEC sp_executesql @vSQLDisableCDC

				--5. Turn ON CDC with the new column
				SET @vSQLEnableCDC = N'EXEC sys.sp_cdc_enable_table ' + CHAR(10) +
				N'@source_schema = ''' + @pSchemaName + ''',' + CHAR(10) +
				N'@source_name = ''' + @pTableName + ''',' + CHAR(10) +
				N'@role_name = NULL;' + CHAR(10) + CHAR(13)
				
				PRINT @vSQLEnableCDC 
				EXEC sp_executesql @vSQLEnableCDC

				IF @vDDLTriggerExists = 1
				BEGIN
					SET @vSQLEnableDDLTrigger = N'ENABLE TRIGGER '+QUOTENAME(@vDDLTriggerName)+' ON DATABASE;' + CHAR(10) + CHAR(13)
						
					PRINT @vSQLEnableDDLTrigger 
					EXEC sp_executesql @vSQLEnableDDLTrigger 
				END

				--6: Insert values from the temp table back into the new CDC Table
				SET @vSQLInsertColumnList = N'DECLARE @vColumnList NVARCHAR(MAX), @InsCommand NVARCHAR(MAX)'				+ CHAR(10) + CHAR(13)+
											N'SELECT @vColumnList = '														+ CHAR(10) +
											N'('																			+ CHAR(10) +
											N'SELECT STRING_AGG (QUOTENAME(column_name), '','' )'							+ CHAR(10) +
											N'FROM cdc.captured_columns'													+ CHAR(10) +
											N'WHERE object_id = OBJECT_ID(''cdc.'+@vCDCTableName+''')'						+ CHAR(10) +
											N')'																			+ CHAR(10) + CHAR(13)+
											

											N'SELECT @InsCommand = '														+ CHAR(10) +
											N'''INSERT INTO cdc.' + @vCDCTableName + ''										+ CHAR(10) +
											N'SELECT __$start_lsn, __$end_lsn, __$seqval, __$operation, __$update_mask,''+' + CHAR(10) +
											N'@vColumnList+'																+ CHAR(10) +
											N''',__$command_id '															+ CHAR(10) +
											N'FROM ' + @vTempTableName + ''''												+ CHAR(10) + CHAR(13)+

											N'PRINT @InsCommand'															+ CHAR(10) +
											N'EXEC sp_executesql @InsCommand'												+ CHAR(10) + CHAR(13)
											
				PRINT @vSQLInsertColumnList
				EXEC sp_executesql @vSQLInsertColumnList
				

				SET @vSQLDropTempTable =	N'DROP TABLE IF EXISTS ' + @vTempTableName + ';' + CHAR(10) + CHAR(13) 

				PRINT @vSQLDropTempTable 
				EXEC sp_executesql @vSQLDropTempTable 

				COMMIT TRANSACTION AddColumnsToCDCTable;
			
			END

		END TRY

		BEGIN CATCH

			ROLLBACK TRANSACTION AddColumnsToCDCTable
			SELECT   
				ERROR_NUMBER() AS ErrorNumber  
				,ERROR_SEVERITY() AS ErrorSeverity  
				,ERROR_STATE() AS ErrorState  
				,ERROR_PROCEDURE() AS ErrorProcedure  
				,ERROR_LINE() AS ErrorLine  
				,ERROR_MESSAGE() AS ErrorMessage; 

			IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION AddColumnsToCDCTable; 

		END CATCH


	--start capture job again
	EXEC sys.sp_cdc_start_job @job_type = N'capture';

END

GO
  
