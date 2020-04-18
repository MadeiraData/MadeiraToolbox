USE [DB_DBA]
GO

IF SCHEMA_ID('Report') IS NULL 
	BEGIN 
		DECLARE @Command NVARCHAR(MAX) = N'CREATE SCHEMA [Report]';
		EXEC (@Command);
	END;

GO
-- =============================================
-- Author:		David Sinai
-- Create date: 11-11-2014
-- Description:	the procedure Checks for Low disk space
-- =============================================
CREATE PROCEDURE [Report].[usp_LowDiskSpace]
	@DebugMode BIT = 0,
	@HTMLTable NVARCHAR(MAX) OUTPUT,
	@Threshold INT = 30  -- free percentage
AS
BEGIN
	BEGIN TRY 

		-----------------------------Create the result table-----------------------------
		
		IF OBJECT_ID ('tempdb..#Drives') IS NOT NULL 
			BEGIN 
				DROP TABLE #Drives
				RAISERROR('the #Drives table already exist',0,0) WITH NOWAIT;
			END 

		IF @Threshold > 100 
			SET @Threshold = 100
		
		IF @Threshold < 0 
			SET @Threshold = 0
		
		DECLARE @SpaceAvailableThreshold DECIMAL(3,2) = CAST((CAST(@Threshold AS DECIMAL(5,2))/100.00) AS DECIMAL (3,2))

        SELECT 
			[Drive Letter] = drive,
			[Drive Size MB] = DriveSizeMB,
			[Space Available MB] = SpaceAvailableMB,
			[Space Available Percent] = N'<font color = "red">'+CAST (SpaceAvailablePercent AS NVARCHAR(MAX)) +N'% </font>'
		INTO #Drives
		FROM 
		(
			SELECT DISTINCT 
				Result.Drive,
				Result.DriveSizeMB,
				Result.SpaceAvailableMB,
				SpaceAvailablePercent = CAST (CAST ((CAST (Result.SpaceAvailableMB AS DECIMAL(38,5))/CAST (Result.DriveSizeMB AS DECIMAL(38,5))) AS DECIMAL(38,5))*100.00  AS DECIMAL(5,2))
			FROM sys.master_files AS f
			CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id)
			CROSS APPLY (SELECT Drive = LEFT(volume_mount_point,1),	DriveSizeMB = total_bytes/1024/1024,	SpaceAvailableMB = available_bytes/1024/1024)Result
		)T
		WHERE 
			SpaceAvailableMB <= DriveSizeMB*(@SpaceAvailableThreshold)
		ORDER BY drive ASC 
		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			[Drive Letter],[Drive Size MB],[Space Available MB],[Space Available Percent]
		FROM #Drives																				
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#Drives',
			@Header =N'Low Disk Space',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = @DebugMode
		---------------------------------------------------------------------------------
		DROP TABLE #Drives

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			IF OBJECT_ID ('tempdb..#Drives') IS NOT NULL 
				BEGIN 
					DROP TABLE #Drives;
				END;
			DECLARE @ErrorMessage NVARCHAR(4000);
			DECLARE @ErrorSeverity INT;
			DECLARE @ErrorState INT;
  
			SELECT 
				@ErrorMessage = ERROR_MESSAGE(),
				@ErrorSeverity = ERROR_SEVERITY(),
				@ErrorState = ERROR_STATE();
  
			RAISERROR (@ErrorMessage,@ErrorSeverity, @ErrorState );
	END CATCH 
END
