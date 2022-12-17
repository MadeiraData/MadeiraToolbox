/*
Find all instances of a JSON element (key) across all paths inside a JSON document
==================================================================================
Author: Eitan Blumin
Date: 2022-12-17
Description:
	Normally, when you want to get a JSON element in SQL Server, you must specify
	for the JSON_MODIFY function an explicit path which would be affected.
	But this wouldn't be possible if you don't necessarily know in advance the JSON path
	where the relevant element/key is located.

	This is the purpose of this function. It will recursively traverse across all paths
	inside the specified JSON document, and find all keys matching the specified element to find.

RESULT (table):
	jpath	nvarchar(4000)	| The JSON path where the element was located inside the document.
	jvalue	nvarchar(MAX)	| The current value of the specified JSON element.


Example execution:

DECLARE
	  @JDoc nvarchar(MAX) = N'{"page": {"content": {  "type": "List",  "items": [{"row": [  {"type": "empty","height": "5vh"  }]},{"row": [  {"type": "label","text": "Hello World 1","color": "#000000","font_size": "27px","font_weight": "bold"  }]},{"row": [  {"type": "empty","height": "1vh"  }]},{"row": [  {"type": "table","text_align": "left","table": {"first_item": "Item 1","top_item": "","bottom_item": "","last_item": "12354568"},"font_size": "14px"  }]},{"row": [  {"type": "separator","border_top_color": "#C0C0C0","border_top_style": "solid","border_top_width": "1px"  }]},{"row": [  {"type": "table","text_align": "left","table": {"first_item": "Item 2","top_item": "","bottom_item": "","last_item": "John Doe"},"font_size": "14px"  }]},{"row": [  {"type": "separator","border_top_color": "#C0C0C0","border_top_style": "solid","border_top_width": "1px"  }]},{"row": [  {"type": "table","text_align": "left","table": {"first_item": "Item 3","top_item": "","bottom_item": "","last_item": "Jane Doe"},"font_size": "14px"  }]},{"row": [  {"type": "separator","border_top_color": "#C0C0C0","border_top_style": "solid","border_top_width": "1px"  }]},{"row": [  {"type": "table","text_align": "left","table": {"first_item": "Adress","top_item": "","bottom_item": "","last_item": "51 Elm St"},"font_size": "14px"  }]},{"row": [  {"type": "separator","border_top_color": "#C0C0C0","border_top_style": "solid","border_top_width": "1px"  }]},{"row": [  {"type": "table","text_align": "left","table": {"first_item": "City","top_item": "","bottom_item": "","last_item": "San Francisco"},"font_size": "14px"  }]},{"row": [  {"type": "separator","border_top_color": "#C0C0C0","border_top_style": "solid","border_top_width": "1px"  }]},{"row": [  {"type": "table","text_align": "left","table": {"first_item": "State, Zip","top_item": "","bottom_item": "","last_item": "CA, 90001"},"font_size": "14px"  }]},{"row": [  {"type": "empty","height": "20vh"  }]},{"row": [  {"type": "action_button","background": "#09374E","text": "Next","color": "#ffffff","font_size": "16px","font_weight": "bold","text_align": "center","align": "center","action": "submit_value","width": "100%","submit_val": "Submit"  }]}  ]},"alignment": "left","screen_category": "","screen_name": "newScreen1","header": {  "background": "#09374E",  "logo_alignment": "center"},"app_icon": "./static/img/logo/icon.svg","back_button": {  "submit_val": "back",  "color": "#ffffff",  "font_size": "25px"}}  }'
	, @jElementToFind nvarchar(4000) = N'type'

SELECT jpath, jvalue
FROM dbo.JSON_FindElementInAllPaths(@JDoc, @jElementToFind)

*/
CREATE FUNCTION dbo.JSON_FindElementInAllPaths
(
	  @JDoc nvarchar(MAX)
	, @jElementToFind nvarchar(4000)
)
RETURNS @Paths table(jpath nvarchar(4000) NOT NULL, jvalue nvarchar(MAX) NOT NULL)
AS
BEGIN

IF @JDoc IS NULL OR @jElementToFind IS NULL RETURN;

DECLARE @CurrPaths AS table (jkey sysname, jvalue nvarchar(MAX) NULL, jtype int, jpath nvarchar(4000) NULL);
INSERT INTO @CurrPaths (jkey, jvalue, jtype, jpath)
SELECT N'$', @JDoc, 5, N'$'

WHILE 1=1
BEGIN
	DECLARE @CurrPath nvarchar(4000), @CurrType int
	SELECT TOP (1) @CurrPath = jpath, @CurrType = jtype
	FROM @CurrPaths
	WHERE jtype IN (4, 5)
	
	IF @@ROWCOUNT = 0 BREAK;

	INSERT INTO @Paths(jpath, jvalue)
	SELECT @CurrPath + CASE WHEN @CurrType = 5 THEN N'.' + [Key] ELSE QUOTENAME([Key]) END, [Value]
	FROM OPENJSON(@JDoc, @CurrPath)
	WHERE [Key] = @jElementToFind

	DELETE FROM @CurrPaths
	WHERE jtype IN (4, 5) AND @CurrPath = jpath

	INSERT INTO @CurrPaths (jkey, jvalue, jtype, jpath)
	SELECT [Key], Value, Type, @CurrPath + CASE WHEN @CurrType = 5 THEN N'.' + [Key] ELSE QUOTENAME([Key]) END
	FROM OPENJSON(@JDoc, @CurrPath)
	WHERE [Type] IN (4, 5);

	SET @CurrPath = NULL;
END

RETURN;
END