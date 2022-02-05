/*========================================================================================================================
-- Description:	Determining the SET options for the current session in SQL Server
-- Scope:		Session
-- Author:		Guy Glantser, Madeira Data Solutions
-- Create Date:	07/11/2021
-- Last Update: 07/11/2021
-- Applies To:	SQL Server (all versions), Azure SQL Database, Azure SQL Managed Instance
=========================================================================================================================*/

DECLARE @SetOptions AS INT = @@OPTIONS;

SELECT
	OptionName			= OptionName ,
	OptionValue			=
		CASE
			WHEN BitLocation = @SetOptions & BitLocation
				THEN N'ON'
			ELSE
				N'OFF'
		END ,
	OptionDescription	= OptionDescription
FROM
	(
		VALUES
			(N'DISABLE_DEF_CNST_CHK' , 1 , N'Controls interim or deferred constraint checking') ,
			(N'IMPLICIT_TRANSACTIONS' , 2 , N'For dblib network library connections, controls whether a transaction is started implicitly when a statement is executed. The IMPLICIT_TRANSACTIONS setting has no effect on ODBC or OLEDB connections.') ,
			(N'CURSOR_CLOSE_ON_COMMIT' , 4 , N'Controls behavior of cursors after a commit operation has been performed') ,
			(N'ANSI_WARNINGS' , 8 , N'Controls truncation and NULL in aggregate warnings') ,
			(N'ANSI_PADDING' , 16 , N'Controls padding of fixed-length variables') ,
			(N'ANSI_NULLS' , 32 , N'Controls NULL handling when using equality operators') ,
			(N'ARITHABORT' , 64 , N'Terminates a query when an overflow or divide-by-zero error occurs during query execution') ,
			(N'ARITHIGNORE' , 128 , N'Returns NULL when an overflow or divide-by-zero error occurs during a query') ,
			(N'QUOTED_IDENTIFIER' , 256 , N'Differentiates between single and double quotation marks when evaluating an expression') ,
			(N'NOCOUNT' , 512 , N'Turns off the message returned at the end of each statement that states how many rows were affected') ,
			(N'ANSI_NULL_DFLT_ON' , 1024 , N'Alters the session''s behavior to use ANSI compatibility for nullability. New columns defined without explicit nullability are defined to allow nulls.') ,
			(N'ANSI_NULL_DFLT_OFF' , 2048 , N'Alters the session''s behavior not to use ANSI compatibility for nullability. New columns defined without explicit nullability do not allow nulls.') ,
			(N'CONCAT_NULL_YIELDS_NULL' , 4096 , N'Returns NULL when concatenating a NULL value with a string') ,
			(N'NUMERIC_ROUNDABORT' , 8192 , N'Generates an error when a loss of precision occurs in an expression') ,
			(N'XACT_ABORT' , 16384 , N'Rolls back a transaction if a Transact-SQL statement raises a run-time error')
	)
	AS
		SetOptions (OptionName , BitLocation , OptionDescription)
ORDER BY
	BitLocation ASC;
GO
