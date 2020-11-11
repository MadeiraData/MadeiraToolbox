/*========================================================================================================================

Description:	The stored procedure created by this script prints a text of any size, even if the size is greater 
				than 8,000 bytes, which is the limit of the regular PRINT statement. It splits the large text into 
				chunks of up to 4,000 characters, and then uses the regular PRINT statement. It searches for line breaks in 
				the text, and prefers the split to occur where there are line breaks, so that the output is more readbale.
Scope:			N/A
Author:			Guy Glantser
Created:		10/12/2011
Last Updated:	11/11/2020
Notes:			N/A

=========================================================================================================================*/

CREATE PROCEDURE
	dbo.PrintMax
(
	@inLargeText AS NVARCHAR(MAX)
)
AS


-- Declare the variables

DECLARE
	@nvcReversedData	AS NVARCHAR(MAX) ,
	@intLineBreakIndex	AS INT ,
	@intSearchLength	AS INT				= 4000;


-- Print chunks of up to 4,000 characters

WHILE
	LEN (@inLargeText) > @intSearchLength
BEGIN


	-- Find the last line break in the current chunk, if such exists

	SET @nvcReversedData	= LEFT (@inLargeText , @intSearchLength);
	SET @nvcReversedData	= REVERSE (@nvcReversedData);
	SET @intLineBreakIndex	= CHARINDEX (CHAR(10) + CHAR(13) , @nvcReversedData);


	-- Print the current chunk up to the last line break (or the whole chunk, if there is no line break)

	PRINT LEFT (@inLargeText , @intSearchLength - @intLineBreakIndex + 1);


	-- Trim the printed chunk

	IF
		@intLineBreakIndex = 0
	BEGIN

		SET @inLargeText = RIGHT (@inLargeText , LEN (@inLargeText) - @intSearchLength);

	END
	ELSE	-- @intLineBreakIndex != 0
	BEGIN

		SET @inLargeText = RIGHT (@inLargeText , LEN (@inLargeText) - @intSearchLength + @intLineBreakIndex - 1);

	END;

END;


-- Print the last chunk

IF
	LEN (@inLargeText) > 0
BEGIN

	PRINT @inLargeText;

END;
GO
