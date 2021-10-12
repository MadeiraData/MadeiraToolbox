-- more info: https://www.mssqltips.com/sqlservertutorial/9217/sql-server-release-dates-and-lifecycle/
-- latest product lifecycle details: https://docs.microsoft.com/lifecycle/products/?products=sql-server
SELECT
N'SQL Server version ' + v.SQLVersion + N' mainstream support ends on '
+ v.EoMainstream + N', and extended support ends on ' + v.EoExtended
+ N'. Please consider upgrading to the latest SQL version to enjoy bug fixes, performance improvements, and security updates for as long as possible.'
, v.EoMainstream
FROM (VALUES
 ('2012', 11, '7/11/2017'	, '7/12/2022')
,('2014', 12, '7/9/2019'	, '7/9/2024')
,('2016', 13, '7/13/2021'	, '7/14/2026')
,('2017', 14, '10/11/2022'	, '10/12/2027')
,('2019', 15, '1/7/2025'	, '1/8/2030')
) AS v(SQLVersion, MajorVersion, EoMainstream, EoExtended)
WHERE 1=1
AND CONVERT(DATETIME, v.EoMainstream, 101) < GETDATE()
AND v.MajorVersion = CAST(SERVERPROPERTY('ProductMajorVersion') AS INT)
AND CAST(SERVERPROPERTY('Edition') AS varchar(100)) <> 'SQL Azure'
OPTION(RECOMPILE);