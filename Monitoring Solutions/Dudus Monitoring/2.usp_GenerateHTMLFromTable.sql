USE [DB_DBA]
GO
-- =============================================
-- Author:		David Sinai
-- Create date: 11-11-2014
-- Description:	the procedure returns an HTML table for a specified temp table name 
-- =============================================
CREATE PROCEDURE [dbo].[usp_GenerateHTMLFromTable]
	@tableName SYSNAME,
	@Header NVARCHAR(4000),
	@HTML NVARCHAR(MAX) OUTPUT,
	@DebugMode BIT = 0
AS 

BEGIN 
BEGIN TRY 
		----------------------------Declare Variables----------------------------
		DECLARE @Command NVARCHAR(MAX)
		DECLARE @ColumnsHTMLHeader NVARCHAR(MAX)
		DECLARE @TableHTML NVARCHAR(MAX)
		-------------------------------------------------------------------------

		IF OBJECT_ID ('tempdb..'+@tableName) IS NULL 
			RAISERROR('the specified table isnt exist',16,2) WITH NOWAIT
		
		DECLARE @BIT BIT = 0
		SET @Command = N'SELECT TOP 1 @BIT=1 FROM '+@tableName
		EXEC SP_EXECUTESQL @Command, N'@BIT BIT OUTPUT',@BIT=@BIT OUTPUT
		
		IF @BIT = 0
			RETURN
		----------------------------Generate Column headers HTML----------------------------
		SET @Command = NULL
		
		SELECT 
			@Command = COALESCE (@Command+',','')+
				'th = '''+name+''''
		FROM [tempdb].[sys].[columns] 
		WHERE object_id = OBJECT_ID(N'tempdb..'+@tableName)
		ORDER BY column_id

		SELECT @Command = N'SELECT @ColumnsHTMLHeader = CONVERT(NVARCHAR(MAX),(SELECT '+@Command+N' FOR XML RAW (''tr''),ELEMENTS,TYPE),1)'

		EXEC SP_EXECUTESQL @Command,N'@ColumnsHTMLHeader  NVARCHAR(MAX) OUTPUT',@ColumnsHTMLHeader=@ColumnsHTMLHeader OUTPUT 
		
		IF @DebugMode = 1 
			SELECT @ColumnsHTMLHeader 
		------------------------------------------------------------------------------------

		----------------------------Generate the table HTML----------------------------
		SET @Command = NULL;
		SELECT 
			@Command = COALESCE (@Command+',','')+
				'td = ISNULL(CAST('+QUOTENAME([columns].[name])+' AS ' + CASE WHEN [types].[collation_name] Is NOT NULL THEN N'XML' ELSE N'NVARCHAR(MAX)' END +  N' ),'''')'
		FROM [tempdb].[sys].[columns] 
		JOIN [tempdb].[sys].[types] ON  [columns].[user_type_id] = [types].[user_type_id]
		WHERE object_id = OBJECT_ID(N'tempdb..'+@tableName)
		ORDER BY [columns].[column_id]

		SELECT @Command = N'SELECT @TableHTML = ISNULL(CONVERT(NVARCHAR(MAX),(SELECT '+@Command+N' FROM ['+@tableName+'] FOR XML RAW (''tr''),ELEMENTS,TYPE),1),'''')'

		IF @DebugMode = 1 
			SELECT @TableHTML AS N'@TableHTML',@Command AS N'@Command'

		EXEC SP_EXECUTESQL @Command,N'@TableHTML  NVARCHAR(MAX) OUTPUT',@TableHTML=@TableHTML OUTPUT 

		-------------------------------------------------------------------------------

		----------------------------Assemble the table----------------------------
		SET @HTML = N'<p></br></br><h2>'+@Header+'</h2></p></br><table border = "1" width = "100%">'+@ColumnsHTMLHeader+@TableHTML+'</table>'
		--------------------------------------------------------------------------

END TRY 
BEGIN CATCH 
	IF @@TRANCOUNT > 1 
		ROLLBACK; 
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