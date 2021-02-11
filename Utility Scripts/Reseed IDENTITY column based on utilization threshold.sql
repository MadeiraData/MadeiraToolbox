/*
=========================================================================
Reseed IDENTITY column based on utilization threshold
=========================================================================
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date: 2021-02-11
Description:
	This script automatically reseeds the identity of a specified
	table, if its current last value exceeds a specified
	utilization percentage of its data type.
	The script also verifies that the current minimum value
	in the table is high enough for "breathing room",
	based on a specified minimum value.
=========================================================================
*/
SET NOCOUNT, ANSI_NULLS, QUOTED_IDENTIFIER ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
	 @TableName			SYSNAME		= N'[dbo].[MyTable]'
	,@PercentForReseed		DECIMAL(10,2)	= 70
	,@MinimumValueForBreathingRoom	INT		= 1000000
	,@WhatIf			BIT		= 0


DECLARE @CMD NVARCHAR(MAX), @TableObjectId INT, @MaxIdentValue INT;
DECLARE @MinCurrentValue INT, @MaxCurrentValue INT, @IdentityColumn SYSNAME, @CurrentPercentUsed DECIMAL(10,2);

SET @TableObjectId = OBJECT_ID(@TableName);

IF ISNULL(CONVERT(int, OBJECTPROPERTYEX(@TableObjectId, 'TableHasIdentity')), 0) = 0
BEGIN
	RAISERROR(N'Table "%s" is not a valid table with an IDENTITY column.',16,1,@TableName);
	GOTO Quit;
END

SET @TableName = QUOTENAME(OBJECT_SCHEMA_NAME(@TableObjectId)) + N'.' + QUOTENAME(OBJECT_NAME(@TableObjectId))

-- Retrieve meta-data
SELECT 
	@IdentityColumn = columns.name,
	@CurrentPercentUsed = Calc2.Percent_Used,
	@MaxCurrentValue = CAST(last_value AS INT),
	@MaxIdentValue = MaxValue
FROM sys.identity_columns
CROSS APPLY (SELECT MaxValue = POWER(CAST(256 AS BIGINT), identity_columns.max_length - 1) * CAST(127 AS BIGINT) + (POWER(CAST(256 AS BIGINT), identity_columns.max_length - 1) - 1)) Calc1
CROSS APPLY (SELECT Percent_Used = CAST(CAST(last_value AS BIGINT) *100./MaxValue AS DECIMAL(10, 2))) Calc2
INNER JOIN sys.columns ON identity_columns.object_id = columns.object_id AND identity_columns.column_id = columns.column_id
WHERE identity_columns.object_id = @TableObjectId

-- Check what's the current minimum Id value
SET @CMD = N'SELECT @MinCurrentValue = MIN(' + QUOTENAME(@IdentityColumn) + N') FROM ' + @TableName + N' WITH(NOLOCK);'

EXEC sp_executesql @CMD, N'@MinCurrentValue INT OUTPUT', @MinCurrentValue OUTPUT;
DECLARE @PercentUsedString NVARCHAR(50);
SET @PercentUsedString = CONVERT(nvarchar, @CurrentPercentUsed)

RAISERROR(N'"%s" identity utilization percent: %s (%d out of %d). Current minimum value: %d', 0, 1, @TableName, @PercentUsedString, @MaxCurrentValue, @MaxIdentValue, @MinCurrentValue) WITH NOWAIT;

IF @CurrentPercentUsed >= @PercentForReseed
BEGIN
	IF @MinCurrentValue < @MinimumValueForBreathingRoom
		RAISERROR(N'Reseeding is needed, but there is not enough breathing room for reseed!', 16, 1);
	ELSE
	BEGIN
		IF @WhatIf = 1
			RAISERROR(N'Reseeding "%s" is needed!', 0, 1, @TableName) WITH NOWAIT;
		ELSE
		BEGIN
			RAISERROR(N'Reseeding "%s"...', 0, 1, @TableName) WITH NOWAIT;
			DBCC CHECKIDENT(@TableName, RESEED, 0);
		END
	END
END
ELSE
BEGIN
	RAISERROR(N'Reseeding "%s" is not needed.', 0, 1, @TableName) WITH NOWAIT;
END

Quit:
GO
