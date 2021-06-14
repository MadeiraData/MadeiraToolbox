/*
================================================
Extended Properties as Database Global Variables
================================================
Author: Eitan Blumin | https://madeiradata.com | https://eitanblumin.com
Date: 2021-06-04
Description:
	Use this sample script as a template or starting point
	for when you want to utilize extended properties
	to save and retrieve values as if using "global" variables
	at the database level.
*/
DECLARE   @PreviousValue datetime
	, @NewValue datetime
	, @ExtendedPropertyName sysname = N'My_Database_Global_Var'

-- Retrieve a value: (don't forget to convert to the correct data type)
SELECT @PreviousValue = CONVERT(datetime, [value])
FROM sys.extended_properties
WHERE [name] = @ExtendedPropertyName

/* TODO: do something here with @PreviousValue and @NewValue */

-- Save a value:
IF NOT EXISTS
(
	SELECT *
	FROM sys.extended_properties
	WHERE [name] = @ExtendedPropertyName
)
BEGIN
	EXEC sp_addextendedproperty @name = @ExtendedPropertyName, @value = @NewValue;  
END
ELSE
BEGIN
	EXEC sp_updateextendedproperty @name = @ExtendedPropertyName, @value = @NewValue;
END
