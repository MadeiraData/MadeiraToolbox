 ----------------------------------------------------------------------------------
 -- Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
 -- Date: 26/06/18
 -- Description:
 --		Compares server level objects and definitions as outputted by the first script (InstancePropertiesGenerateForCompare.sql).
 --
 -- Instructions:
 --		1. Run InstancePropertiesGenerateForCompare.sql on each server. Save output to a CSV file.
 --		2. Use this script ( InstancePropertiesComparison.sql ) to load the files into a temp table, and output any differences.
 --		Don't forget to change file paths accordingly.
 ----------------------------------------------------------------------------------

--USE [tempdb]

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

-- Create Table for Comparisons
IF OBJECT_ID('tempdb..#InstanceProperties') IS NOT NULL DROP TABLE #InstanceProperties;

CREATE TABLE #InstanceProperties
(
	ServerName VARCHAR(300) COLLATE database_default NULL,
	Category VARCHAR(100) COLLATE database_default NULL,
	ItemName VARCHAR(500) COLLATE database_default NULL,
	PropertyName VARCHAR(500) COLLATE database_default NULL,
	PropertyValue VARCHAR(8000) COLLATE database_default NULL
);
CREATE CLUSTERED INDEX IX ON #InstanceProperties (ServerName, Category, ItemName, PropertyName);

DECLARE Paths CURSOR
LOCAL FAST_FORWARD
FOR
SELECT CsvPath
FROM (VALUES
 ('C:\temp\SQLDB1_InstanceProperties.csv')
,('C:\temp\SQLDB2_InstanceProperties.csv')
,('C:\temp\SQLDB3_InstanceProperties.csv')
,('C:\temp\SQLDB4_InstanceProperties.csv')
) AS v(CsvPath)

DECLARE @CurrPath nvarchar(4000), @Cmd nvarchar(MAX);

OPEN Paths;

WHILE 1=1
BEGIN
	FETCH NEXT FROM Paths INTO @CurrPath;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @Cmd = N'BULK INSERT #InstanceProperties
FROM ''' + @CurrPath + N'''
WITH (CODEPAGE = ''65001'', FIELDTERMINATOR='','')'
	PRINT @Cmd;
	EXEC (@Cmd);
END

CLOSE Paths;
DEALLOCATE Paths;


-- Cleanup unicode remnants
UPDATE #InstanceProperties SET ServerName = REPLACE(ServerName, N'ן»¿', '')
WHERE ServerName LIKE 'ן»¿%'


--SELECT * FROM #InstanceProperties -- debug


-- Perform comparisons

DECLARE @ServersCount int

SELECT @ServersCount = COUNT(DISTINCT ServerName) FROM #InstanceProperties WHERE ServerName IS NOT NULL;

DECLARE @MatchedItems AS TABLE (Category VARCHAR(100), ItemName VARCHAR(500), PropertyName VARCHAR(500) NULL, UNIQUE (Category, ItemName, PropertyName))

INSERT INTO @MatchedItems
SELECT Category, ItemName, PropertyName
FROM #InstanceProperties
GROUP BY Category, ItemName, PropertyName
HAVING COUNT(DISTINCT ServerName) = @ServersCount
OPTION(RECOMPILE)

;
WITH Srv AS
(SELECT DISTINCT ServerName FROM #InstanceProperties WHERE ServerName IS NOT NULL)
, ItemNonMatches AS
(
	SELECT Category, ItemName
	FROM #InstanceProperties
	WHERE
	-- Ignore 2nd level categories
		Category NOT LIKE '%: %'
	GROUP BY Category, ItemName
	HAVING COUNT(DISTINCT ServerName) < @ServersCount
)
, PropertiesNonMatches AS
(
	SELECT Category, ItemName, PropertyName
	FROM #InstanceProperties AS i
	WHERE NOT EXISTS (SELECT NULL FROM ItemNonMatches AS inm WHERE i.Category = inm.Category AND i.ItemName = inm.ItemName)
	AND i.PropertyName IS NOT NULL
	GROUP BY Category, ItemName, PropertyName
	HAVING COUNT(DISTINCT ServerName) < @ServersCount
)
SELECT Issue = 'Missing Item'
, Category, ItemName, PropertyName = CONVERT(varchar(500),NULL)
, Details = (SELECT missing = (SELECT [@ServerName] = ServerName
	 FROM Srv
	 WHERE NOT EXISTS
	 (SELECT NULL
	  FROM #InstanceProperties AS i
	  WHERE i.ServerName = Srv.ServerName
	  AND i.Category = inm.Category
	  AND i.ItemName = inm.ItemName
	 )
	 FOR XML PATH('val'), TYPE)
	 , existing = (
	  SELECT [@ServerName] = ServerName, [@PropertyName] = i.PropertyName, [text()] = i.[PropertyValue]
	  FROM #InstanceProperties AS i
	  WHERE i.Category = inm.Category
	  AND i.ItemName = inm.ItemName
	  FOR XML PATH('val'), TYPE
	  )
	 FOR XML PATH('details'), TYPE
	 )
FROM ItemNonMatches AS inm

UNION ALL

SELECT Issue = 'Missing Property'
, Category, ItemName, PropertyName = inm.PropertyName
, Details = (SELECT missing = (SELECT [@ServerName] = ServerName
	 FROM Srv
	 WHERE NOT EXISTS
	 (SELECT NULL
	  FROM #InstanceProperties AS i
	  WHERE i.ServerName = Srv.ServerName
	  AND i.Category = inm.Category
	  AND i.ItemName = inm.ItemName
	  AND i.PropertyName = inm.PropertyName
	 )
	 FOR XML PATH('val'), TYPE)
	 , existing = (
	  SELECT [@ServerName] = ServerName, [@PropertyName] = i.PropertyName, [text()] = i.[PropertyValue]
	  FROM #InstanceProperties AS i
	  WHERE i.Category = inm.Category
	  AND i.ItemName = inm.ItemName
	  AND i.PropertyName = inm.PropertyName
	  FOR XML PATH('val'), TYPE
	  )
	 FOR XML PATH('details'), TYPE
	 )
FROM PropertiesNonMatches AS inm

UNION ALL

SELECT Issue = 'Value is different', Category, ItemName, PropertyName
, Details =
	(SELECT [@ServerName] = ServerName, [text()] = PropertyValue
	 FROM #InstanceProperties AS v
	 WHERE v.Category = i.Category
	 AND v.ItemName = i.ItemName
	 AND v.PropertyName = i.PropertyName
	 FOR XML PATH('val'), TYPE)
FROM #InstanceProperties AS i
WHERE EXISTS (SELECT NULL FROM @MatchedItems MI WHERE MI.Category = i.Category AND MI.ItemName = i.ItemName AND EXISTS (SELECT i.PropertyName INTERSECT SELECT MI.PropertyName))
GROUP BY Category, ItemName, PropertyName
HAVING COUNT(DISTINCT PropertyValue) > 1
OPTION(RECOMPILE)
GO
