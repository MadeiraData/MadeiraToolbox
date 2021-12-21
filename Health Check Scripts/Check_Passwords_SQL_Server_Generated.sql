DECLARE @OutputPassword bit = 0;

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb..#logins') IS NOT NULL DROP TABLE #logins;
CREATE TABLE #logins
(
Deviation nvarchar(4000),
LoginName sysname,
ServerRoles nvarchar(MAX) NULL,
ServerPermissions nvarchar(MAX) NULL,
DBAccess nvarchar(MAX) NULL
)

;WITH NumbersCTE
AS
(
	SELECT CONVERT(nvarchar(MAX),'') AS generatedPwd, 0 AS currLength, Numbers.num AS Pwdmaxlength, V.TxtSkip, L.SkipLocation
	FROM (VALUES
	(3),(4),(5),(6),(7),(8),(9)
	) AS Numbers(num)
	CROSS JOIN
	(VALUES
	(''),(NULL),('0'),('a')
	) AS V(TxtSkip)
	CROSS JOIN
	(VALUES
	('before'),('after')
	) AS L(SkipLocation)

	UNION ALL

	SELECT CTE.generatedPwd
	+ CASE WHEN CTE.SkipLocation = 'before' THEN ISNULL(CTE.TxtSkip, CONVERT(nvarchar(MAX), CTE.currLength + 1)) ELSE '' END
	+ CONVERT(nvarchar(MAX), CTE.currLength + 1)
	+ CASE WHEN CTE.SkipLocation = 'after' THEN ISNULL(CTE.TxtSkip, CONVERT(nvarchar(MAX), CTE.currLength + 1)) ELSE '' END
	, CTE.currLength + 1
	, CTE.Pwdmaxlength
	, CTE.TxtSkip
	, CTE.SkipLocation
	FROM NumbersCTE AS CTE
	WHERE CTE.currLength <= CTE.Pwdmaxlength
	AND LEN(CTE.generatedPwd) < 16
), CommonPermutationWords
AS
(
	SELECT V.TxtWord
	FROM (VALUES
	('123'),('1234'),('12345'),('456'),('654'),
	('asd'),('qwe'),('qwer'),('qwert'),('qwerty'),('asdf'),('zxc'),('abc'),('abcd'),
	('159'),('951'),('753'),('357'),('147'),('741'),('258'),('852'),('369'),('963')
	) AS V(TxtWord)
), CTE_Level2
AS
(
SELECT CTE.generatedPwd
FROM NumbersCTE AS CTE
WHERE CTE.currLength = CTE.Pwdmaxlength
UNION ALL
SELECT '1234567890'
), GeneratedPasswords
AS
(
-- Sorted numbers with and without skip characters
SELECT CTE_Level2.generatedPwd
FROM CTE_Level2

UNION ALL

-- Reversed sorted numbers with and without skip characters
SELECT REVERSE(CTE_Level2.generatedPwd)
FROM CTE_Level2

UNION

-- Replicated text
SELECT REPLICATE(V.TxtChar, Occ.Occurences)
FROM (VALUES
('0',4,10),('1',4,10),('2',4,10),('3',4,10),('4',4,10),('5',4,10),('6',4,10),('7',4,10),('8',4,10),('9',4,10),('a',4,10)
,('123',1,4),('abc',1,4),('a1',1,4),('qwe',1,4),('12',1,4),('21',1,4),('13',1,4),('159',1,3),('456',1,3),('23',1,3),('1234',2,3),('147',1,3),('321',1,3)
,('bla',1,4),('25',2,4),('69',2,4),('100',2,4),('qwerty',1,2),('x',3,8),('asd',1,3)
) AS V(TxtChar,MinOccurences,MaxOccurences)
INNER JOIN
(SELECT TOP (10) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Occurences FROM sys.all_columns) AS Occ
ON Occ.Occurences BETWEEN V.MinOccurences AND V.MaxOccurences

UNION

-- Duplicate and reverse
SELECT V.T + ISNULL(V.Mid,'') + REVERSE(V.T)
FROM (VALUES
('123','4'),
('123',''),
('159',''),
('qwe','')
) AS V(T,Mid)

UNION

-- Common permutations
SELECT a.TxtWord + b.TxtWord
FROM CommonPermutationWords AS a
CROSS JOIN CommonPermutationWords AS b

UNION

-- Common words
SELECT V.TxtWord
FROM (VALUES
 ('password')
,('password1')
,('P@ssw0rd')
,('passw0rd')
,('P@$$w0rd')
,('Pa$$w0rd')
,('passw0rd')
,('Aa123456')
,('1q2w3e4r')
,('sha256')
,('1q2w3e')
,('1qaz2wsx')
,('q1w2e3r4')
,('q1w2e3')
,('1q2w3e4r5t')
,('a123456'),('123456a'),('a1234567'),('a12345')
,('blackcoffee333')
,('qazwsx')
,('letmein')
,('israel')
,('iloveyou')
,('1qazxsw2')
,('142536')
,('147258369')
,('134679')
,('dragon')
,('asdqwe123')
,('12qwaszx')
,('123698745')
,('123654789')
,('zaq12wsx')
,('a1b2c3d4')
,('aaron431')
,('qqww1122')
,('monkey')
,('ginger')
,('jinjer')
,('vampire')
) AS V(TxtWord)
)
INSERT INTO #logins
(Deviation, LoginName, ServerRoles, ServerPermissions)
SELECT 
Deviation = dev.Deviation + CASE WHEN @OutputPassword = 1 THEN N' (' + Pwd + N')' ELSE N'' END
, [LoginName]
, ServerRoles =
STUFF((
	SELECT N', ' + roles.name
	FROM sys.server_role_members AS srm
	INNER JOIN sys.server_principals AS roles ON srm.role_principal_id = roles.principal_id
	WHERE srm.member_principal_id = SUSER_ID(dev.[LoginName])
	FOR XML PATH('')
	), 1, 2, N'')
, ServerPermissions =
STUFF((
	SELECT N', ' + perm.state_desc + N' ' + perm.permission_name + N' ' + perm.class_desc
	FROM sys.server_permissions AS perm
	WHERE perm.grantee_principal_id = SUSER_ID([LoginName])
	FOR XML PATH('')
	), 1, 2, N'')
FROM
(
SELECT 'Empty Password' AS Deviation, RTRIM(name) AS [LoginName], '' AS Pwd
FROM master.sys.sql_logins
WHERE ([password_hash] IS NULL OR PWDCOMPARE('', [password_hash]) = 1)
AND name NOT IN ('MSCRMSqlClrLogin')
AND name NOT LIKE '##MS_%##'
AND is_disabled = 0

UNION ALL

SELECT DISTINCT 'Login Name is the same as Password' AS Deviation, RTRIM(s.name) AS [Name] , RTRIM(RTRIM(s.name)) AS Pwd
FROM master.sys.sql_logins s 
WHERE PWDCOMPARE(RTRIM(RTRIM(s.name)), s.[password_hash]) = 1
AND s.is_disabled = 0

UNION ALL

SELECT DISTINCT N'Weak Password' AS Deviation, RTRIM(s.name) AS [LoginName], RTRIM(RTRIM(d.generatedPwd)) AS Pwd
FROM GeneratedPasswords d
INNER JOIN master.sys.sql_logins s ON PWDCOMPARE(RTRIM(RTRIM(d.generatedPwd)), s.[password_hash]) = 1
WHERE s.is_disabled = 0
) AS dev
OPTION (RECOMPILE); -- avoid saving this in plan cache

DECLARE @CurrDB sysname, @Executor nvarchar(1000), @Cmd nvarchar(MAX), @DBAccess nvarchar(MAX);

SET @Cmd = N'
UPDATE logins
	SET DBAccess = ISNULL(DBAccess + N'', '', N'''') + QUOTENAME(DB_NAME())
	+ ISNULL(N'' ('' +
	STUFF((
		SELECT N'', '' + roles.name
		FROM sys.database_role_members AS drm
		INNER JOIN sys.server_principals AS roles ON drm.role_principal_id = roles.principal_id
		WHERE drm.member_principal_id = dp.principal_id
	FOR XML PATH('''')
	), 1, 2, N'''')
	+ N'')'', N'''')
FROM #logins AS logins
INNER JOIN sys.database_principals AS dp ON dp.sid = SUSER_SID(logins.LoginName)'

DECLARE DBs CURSOR
FAST_FORWARD LOCAL
FOR
SELECT [name]
FROM sys.databases
WHERE HAS_DBACCESS([name]) = 1
AND state = 0

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @DBAccess = NULL;

	SET @Executor = QUOTENAME(@CurrDB) + N'..sp_executesql'
	EXEC @Executor @Cmd WITH RECOMPILE;
END

CLOSE DBs;
DEALLOCATE DBs;

SELECT * FROM #logins;