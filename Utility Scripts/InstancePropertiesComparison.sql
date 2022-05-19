 ----------------------------------------------------------------------------------
 -- Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
 -- Date: 26/06/18
 -- Description:
 --		Compares server level objects and definitions as outputted by the first script (InstancePropertiesGenerateForCompare.sql).
 --
 -- Instructions:
 --		Run InstancePropertiesGenerateForCompare.sql on each server. Save output to a CSV file.
 --		Use this script ( InstancePropertiesComparison.sql ) to load the files into a table, and output any differences
 --		Don't forget to change file paths accordingly.
 -- Disclaimer:
 --		Recommended to run in TEMPDB, unless you want to retain results in the long-term.
 --		Note that this script runs TRUNCATE TABLE if InstanceProperties already exists. 
 --		So if you want long-term retention, you should remove that (lines 32-33).
 ----------------------------------------------------------------------------------

SET NOCOUNT ON;
GO
-- Create Table for Comparisons
IF OBJECT_ID('InstanceProperties') IS NULL
BEGIN
	CREATE TABLE dbo.InstanceProperties
	(
		ServerName VARCHAR(300) COLLATE database_default NULL,
		Category VARCHAR(100) COLLATE database_default NULL,
		ItemName VARCHAR(500) COLLATE database_default NULL,
		PropertyName VARCHAR(500) COLLATE database_default NULL,
		PropertyValue VARCHAR(8000) COLLATE database_default NULL
	);
	CREATE CLUSTERED INDEX IX ON dbo.InstanceProperties (ServerName, Category, ItemName, PropertyName);
END
ELSE
	TRUNCATE TABLE dbo.InstanceProperties;
GO
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

	SET @Cmd = N'BULK INSERT dbo.InstanceProperties
FROM ''' + @CurrPath + N'''
WITH (CODEPAGE = ''65001'', FIELDTERMINATOR='','')'
	PRINT @Cmd;
	EXEC (@Cmd);
END

CLOSE Paths;
DEALLOCATE Paths;

GO

-- Cleanup unicode remnants
UPDATE dbo.InstanceProperties SET ServerName = REPLACE(ServerName, N'ן»¿', '')
WHERE ServerName LIKE 'ן»¿%'
GO

--SELECT * FROM dbo.InstanceProperties -- debug
GO

-- Perform comparisons

DECLARE @ServersCount int

SELECT @ServersCount = COUNT(DISTINCT ServerName) FROM dbo.InstanceProperties WHERE ServerName IS NOT NULL;

DECLARE @MatchedItems AS TABLE (Category VARCHAR(100), ItemName VARCHAR(500), PRIMARY KEY (Category, ItemName))

INSERT INTO @MatchedItems
SELECT Category, ItemName
FROM dbo.InstanceProperties
GROUP BY Category, ItemName
HAVING COUNT(DISTINCT ServerName) = @ServersCount

;
WITH Srv AS
(SELECT DISTINCT ServerName FROM dbo.InstanceProperties WHERE ServerName IS NOT NULL)
, ItemNonMatches AS
(
	SELECT Category, ItemName
	FROM dbo.InstanceProperties
	WHERE
	-- Ignore 2nd level categories
		Category NOT LIKE '%: %'
	GROUP BY Category, ItemName
	HAVING COUNT(DISTINCT ServerName) < @ServersCount
)
SELECT Issue = 'Missing Item'
, Category, ItemName, PropertyName = CONVERT(varchar(500),NULL)
, Details = (SELECT [@ServerName] = ServerName
	 FROM Srv
	 WHERE NOT EXISTS
	 (SELECT NULL
	  FROM dbo.InstanceProperties AS i
	  WHERE i.ServerName = Srv.ServerName
	  AND i.Category = inm.Category
	  AND i.ItemName = inm.ItemName
	 )
	 FOR XML PATH('srv'), TYPE)
FROM ItemNonMatches AS inm

UNION ALL

SELECT Issue = 'Value is different', Category, ItemName, PropertyName
, Details =
	(SELECT [@ServerName] = ServerName, [text()] = PropertyValue
	 FROM dbo.InstanceProperties AS v
	 WHERE v.Category = i.Category
	 AND v.ItemName = i.ItemName
	 AND v.PropertyName = i.PropertyName
	 FOR XML PATH('val'), TYPE)
FROM dbo.InstanceProperties AS i
WHERE EXISTS (SELECT NULL FROM @MatchedItems MI WHERE MI.Category = i.Category AND MI.ItemName = i.ItemName)
GROUP BY Category, ItemName, PropertyName
HAVING COUNT(DISTINCT PropertyValue) > 1
GO
