 ----------------------------------------------------------------------------------
 -- Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
 -- Date: 26/06/18
 -- Description:
 --		Compares server level objects and definitions as outputted by the first script (GenerateInstancePropertiesForCompare.sql).
 --
 -- Instructions:
 --		Run GenerateInstancePropertiesForCompare.sql on "First" server. Save output to a CSV file.
 --		Run GenerateInstancePropertiesForCompare.sql on "Second" server. Save output to a CSV file.
 --		Use this script ( CompareInstanceProperties.sql ) to load the files into a table, and output any differences
 --		Don't forget to change file paths and server names accordingly.
 -- Disclaimer:
 --		Recommended to run in TEMPDB, unless you want to retain results in the long-term.
 --		Note that this script runs TRUNCATE TABLE if InstanceProperties already exists. 
 --		So if you want long-term retention, you should remove that (lines 33-34).
 ----------------------------------------------------------------------------------

SET NOCOUNT ON;
GO
-- Create Table for Comparisons
IF OBJECT_ID('InstanceProperties') IS NULL
BEGIN
	CREATE TABLE dbo.InstanceProperties
	(
		ServerName VARCHAR(300) COLLATE database_default,
		Category VARCHAR(100) COLLATE database_default,
		ItemName VARCHAR(500) COLLATE database_default,
		PropertyName VARCHAR(500) COLLATE database_default,
		PropertyValue VARCHAR(8000) COLLATE database_default
	);
	CREATE CLUSTERED INDEX IX ON InstanceProperties (ServerName, Category, ItemName, PropertyName);
END
ELSE
	TRUNCATE TABLE dbo.InstanceProperties;
GO

BULK INSERT dbo.InstanceProperties
FROM 'C:\temp\SQLDB1_InstanceProperties.csv'
WITH (CODEPAGE = '65001', FIELDTERMINATOR=',')

GO

BULK INSERT dbo.InstanceProperties
FROM 'C:\temp\SQLDB2_InstanceProperties.csv'
WITH (CODEPAGE = '65001', FIELDTERMINATOR=',')

GO

BULK INSERT dbo.InstanceProperties
FROM 'C:\temp\SQLDB3_InstanceProperties.csv'
WITH (CODEPAGE = '65001', FIELDTERMINATOR=',')

GO

BULK INSERT dbo.InstanceProperties
FROM 'C:\temp\SQLDB4_InstanceProperties.csv'
WITH (CODEPAGE = '65001', FIELDTERMINATOR=',')

GO

-- Cleanup unicode remnants
UPDATE dbo.InstanceProperties SET ServerName = REPLACE(ServerName, N'ן»¿', '')
WHERE ServerName LIKE 'ן»¿%'
GO

--SELECT * FROM dbo.InstanceProperties -- debug
GO

-- Perform comparisons (don't forget to change server names accordingly)

DECLARE
	@ServerA VARCHAR(300) = 'SQLDB1',
	@ServerB VARCHAR(300) = 'SQLDB2'

DECLARE @MatchedItems AS TABLE (Category VARCHAR(100), ItemName VARCHAR(500), PRIMARY KEY (Category, ItemName))
;
WITH SrvA AS
(SELECT * FROM dbo.InstanceProperties WHERE ServerName = @ServerA)
, SrvB AS
(SELECT * FROM dbo.InstanceProperties WHERE ServerName = @ServerB)

INSERT INTO @MatchedItems
SELECT DISTINCT ISNULL(A.Category, B.Category) AS Category, ISNULL(A.ItemName, B.ItemName) AS ItemName
FROM SrvA AS A
INNER JOIN SrvB AS B
ON
	A.Category = B.Category
AND A.ItemName = B.ItemName

SELECT ServerA = @ServerA, ServerB = @ServerB, ExecutionTime = GETDATE()
;
WITH SrvA AS
(SELECT * FROM dbo.InstanceProperties WHERE ServerName = @ServerA)
, SrvB AS
(SELECT * FROM dbo.InstanceProperties WHERE ServerName = @ServerB)
, ItemNonMatches AS
(
	SELECT DISTINCT ISNULL(A.Category, B.Category) AS Category, ISNULL(A.ItemName, B.ItemName) AS ItemName, CASE WHEN A.ServerName IS NULL THEN @ServerA ELSE @ServerB END AS MissingOn
	FROM SrvA AS A
	FULL JOIN SrvB AS B
	ON
		A.Category = B.Category
	AND A.ItemName = B.ItemName
	WHERE
		A.ServerName IS NULL
	OR B.ServerName IS NULL
)
SELECT Issue = 'Missing Item in ' + MissingOn, Category, ItemName, PropertyName = CONVERT(varchar(500),NULL)
, ValueOnServerA = CASE WHEN MissingOn = @ServerA THEN 'Missing' ELSE 'Exists' END
, ValueOnServerB = CASE WHEN MissingOn = @ServerB THEN 'Missing' ELSE 'Exists' END
FROM ItemNonMatches
WHERE
-- Ignore 2nd level categories
	Category NOT LIKE '%: %'

UNION ALL

SELECT Issue = 'Value is different', Category = ISNULL(A.Category, B.Category), ItemName = ISNULL(A.ItemName, B.ItemName), PropertyName = ISNULL(A.PropertyName, B.PropertyName)
, ValueOnServerA = A.PropertyValue
, ValueOnServerB = B.PropertyValue
FROM SrvA AS A
FULL OUTER JOIN SrvB AS B
ON
	A.Category = B.Category
AND A.ItemName = B.ItemName
AND A.PropertyName = B.PropertyName
WHERE EXISTS (SELECT * FROM @MatchedItems MI WHERE MI.Category = ISNULL(A.Category, B.Category) AND MI.ItemName = ISNULL(A.ItemName, B.ItemName))
AND (
	A.PropertyValue <> B.PropertyValue
	OR (
		(A.PropertyValue IS NULL OR B.PropertyValue IS NULL)
		AND NOT (A.PropertyValue IS NULL AND B.PropertyValue IS NULL)
	   )
	)
GO
