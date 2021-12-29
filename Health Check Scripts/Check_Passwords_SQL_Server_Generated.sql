/*
Check SQL Logins for Weak Passwords
===================================
Author: Eitan Blumin
Date: 2020-09-02
Weak passwords list is based on: https://github.com/danielmiessler/SecLists/tree/master/Passwords
*/
DECLARE
	@BringThePain		bit = 0
,	@OutputPasswords	bit = 0

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb..#pwd') IS NOT NULL DROP TABLE #pwd;
CREATE TABLE #pwd
(
generatedPwd nvarchar(4000) COLLATE Latin1_General_BIN NOT NULL
);

IF OBJECT_ID('tempdb..#logins') IS NOT NULL DROP TABLE #logins;
CREATE TABLE #logins
(
Deviation varchar(256),
LoginSID varbinary(4000) NULL,
LoginPrincipleId int NULL,
LoginName sysname,
ServerRoles varchar(8000) NULL,
ServerPermissions varchar(8000) NULL,
DBAccess varchar(MAX) NULL
)

IF OBJECT_ID('sys.sql_logins') IS NULL
BEGIN
	PRINT N'This script is not supported on this SQL Server edition.';
	SET NOEXEC ON;
END

DECLARE @RCount int

-- Common passwords
INSERT INTO #pwd WITH (TABLOCKX)
SELECT [value]
FROM (VALUES
 ('1234321')
,('qweewq')
,('101010')
,('10203')
,('123123123')
,('147258369')
,('1qaz2wsx')
,('789456123')
,('jordan23')
,('#SAPassword!')
,('$easyWinArt4')
,('$ei$micMicro')
,('*ARIS!1dm9n#')
,('123698745')
,('134679')
,('142536')
,('1q2w3e')
,('1q2w3e4r')
,('1q2w3e4r5t')
,('20100728')
,('25251325')
,('42Emerson42Eme')
,('5201314')
,('686584')
,('a1b2c3d4')
,('a801016')
,('aaron431')
,('admin')
,('Administrator1')
,('AIMS')
,('alexander')
,('amanda')
,('andrea')
,('andrew')
,('angel1')
,('anhyeuem')
,('anthony')
,('ashley')
,('azerty')
,('bailey')
,('Bangbang123')
,('baseball')
,('basketball')
,('batman')
,('blackcoffee333')
,('blink182')
,('BPMS')
,('buster')
,('butterfly')
,('capassword')
,('Cardio.Perfect')
,('charlie')
,('chatbooks')
,('cheese')
,('chocolate')
,('cic')
,('cic!23456789')
,('computer')
,('cookie')
,('daniel')
,('DBA!sa@EMSDB123')
,('default')
,('Dr8gedog')
,('dragon')
,('evite')
,('family')
,('flower')
,('gabriel')
,('ginger')
,('gnos')
,('hannah')
,('hunter')
,('i2b2demodata')
,('i2b2demodata2')
,('i2b2hive')
,('i2b2metadata')
,('i2b2metadata2')
,('i2b2workdata')
,('i2b2workdata2')
,('israel')
,('jacket025')
,('jakcgt333')
,('jennifer')
,('jessica')
,('jesus1')
,('jinjer')
,('jobandtalent')
,('joshua')
,('justin')
,('killer')
,('letmein')
,('lovely')
,('loveme')
,('M3d!aP0rtal')
,('madison')
,('maggieown')
,('master')
,('matthew')
,('maxadmin')
,('maxreg')
,('medocheck123')
,('michelle')
,('Million2')
,('monkey')
,('MULTIMEDIA')
,('mxintadm')
,('myspace1')
,('naruto')
,('netxms')
,('nicole')
,('ohmnamah23')
,('omgpop')
,('opengts')
,('P@$$w0rd')
,('P@ssw0rd')
,('Pa$$w0rd')
,('party')
,('Pass@123')
,('passw0rd')
,('peanut')
,('pepper')
,('picture1')
,('pokemon')
,('PracticeUser1')
,('purple')
,('pwAdmin')
,('pwddbo')
,('pwPower')
,('pwUser')
,('q1w2e3')
,('q1w2e3r4')
,('robert')
,('RPSsql12345')
,('samantha')
,('Sample123')
,('samsung')
,('SECAdmin1')
,('SecurityMaster08')
,('senha')
,('sha256')
,('shadow')
,('SilkCentral12!34')
,('skf_admin1')
,('soccer')
,('splendidcrm2005')
,('sqlserver')
,('starwars')
,('stream-1')
,('summer')
,('sunshine')
,('superadmin')
,('superman')
,('taylor')
,('thomas')
,('tigger')
,('trinity')
,('trustno1')
,('unknown')
,('V4in$ight')
,('vampire')
,('vantage12!')
,('wasadmin')
,('welcome')
,('whatever')
,('wwAdmin')
,('wwPower')
,('wwUser')
,('x4ivygA51F')
,('yugioh')
,('zing')
,('4128')
,('5150')
,('8675309')
,('abgrtyu')
,('access')
,('access14')
,('action')
,('albert')
,('alex')
,('alexis')
,('amateur')
,('angel')
,('angela')
,('angels')
,('animal')
,('apollo')
,('apple')
,('apples')
,('arsenal')
,('arthur')
,('asshole')
,('august')
,('austin')
,('baby')
,('badboy')
,('banana')
,('barney')
,('beach')
,('bear')
,('beaver')
,('beavis')
,('beer')
,('bigcock')
,('bigdaddy')
,('bigdick')
,('bigdog')
,('bigtits')
,('bill')
,('billy')
,('birdie')
,('bitch')
,('bitches')
,('biteme')
,('black')
,('blazer')
,('blonde')
,('blondes')
,('blowjob')
,('blowme')
,('blue')
,('bond007')
,('bonnie')
,('booboo')
,('boobs')
,('booger')
,('boomer')
,('booty')
,('boston')
,('brandon')
,('brandy')
,('braves')
,('brazil')
,('brian')
,('bronco')
,('broncos')
,('bubba')
,('buddy')
,('bulldog')
,('butter')
,('butthead')
,('calvin')
,('camaro')
,('cameron')
,('canada')
,('captain')
,('carlos')
,('carter')
,('casper')
,('charles')
,('chelsea')
,('chester')
,('chevy')
,('chicago')
,('chicken')
,('chris')
,('cocacola')
,('cock')
,('coffee')
,('college')
,('compaq')
,('cool')
,('cooper')
,('corvette')
,('cowboy')
,('cowboys')
,('cream')
,('crystal')
,('cumming')
,('cumshot')
,('cunt')
,('dakota')
,('dallas')
,('danielle')
,('dave')
,('david')
,('debbie')
,('dennis')
,('diablo')
,('diamond')
,('dick')
,('dirty')
,('doctor')
,('doggie')
,('dolphin')
,('dolphins')
,('donald')
,('dreams')
,('driver')
,('eagle')
,('eagle1')
,('eagles')
,('edward')
,('einstein')
,('enjoy')
,('enter')
,('eric')
,('erotic')
,('extreme')
,('falcon')
,('fender')
,('ferrari')
,('fire')
,('firebird')
,('fish')
,('fishing')
,('florida')
,('flyers')
,('ford')
,('forever')
,('frank')
,('fred')
,('freddy')
,('freedom')
,('fuck')
,('fucked')
,('fucker')
,('fucking')
,('fuckme')
,('gandalf')
,('gateway')
,('gators')
,('gemini')
,('george')
,('giants')
,('girl')
,('girls')
,('golden')
,('golf')
,('golfer')
,('gordon')
,('great')
,('green')
,('gregory')
,('guitar')
,('gunner')
,('hammer')
,('happy')
,('hardcore')
,('harley')
,('heather')
,('helpme')
,('hentai')
,('hockey')
,('hooters')
,('horney')
,('horny')
,('hotdog')
,('house')
,('hunting')
,('iceman')
,('internet')
,('iwantu')
,('jack')
,('jackie')
,('jackson')
,('jaguar')
,('jake')
,('james')
,('japan')
,('jasmine')
,('jason')
,('jasper')
,('jeremy')
,('john')
,('johnny')
,('johnson')
,('joseph')
,('juice')
,('junior')
,('kelly')
,('kevin')
,('king')
,('kitty')
,('knight')
,('ladies')
,('lakers')
,('lauren')
,('leather')
,('legend')
,('little')
,('london')
,('lover')
,('lovers')
,('lucky')
,('maddog')
,('maggie')
,('magic')
,('magnum')
,('marine')
,('mark')
,('marlboro')
,('martin')
,('marvin')
,('matrix')
,('matt')
,('maverick')
,('maxwell')
,('melissa')
,('member')
,('mercedes')
,('merlin')
,('mickey')
,('midnight')
,('mike')
,('miller')
,('mine')
,('mistress')
,('money')
,('monica')
,('monster')
,('morgan')
,('mother')
,('mountain')
,('movie')
,('muffin')
,('murphy')
,('music')
,('mustang')
,('naked')
,('nascar')
,('nathan')
,('naughty')
,('ncc1701')
,('newyork')
,('nicholas')
,('nipple')
,('nipples')
,('oliver')
,('orange')
,('ou812')
,('packers')
,('panther')
,('panties')
,('paris')
,('parker')
,('pass')
,('patrick')
,('paul')
,('peaches')
,('penis')
,('peter')
,('phantom')
,('phoenix')
,('player')
,('please')
,('pookie')
,('porn')
,('porno')
,('porsche')
,('power')
,('prince')
,('private')
,('pussies')
,('pussy')
,('rabbit')
,('rachel')
,('racing')
,('raiders')
,('rainbow')
,('ranger')
,('rangers')
,('rebecca')
,('redskins')
,('redsox')
,('redwings')
,('richard')
,('rock')
,('rocket')
,('rosebud')
,('runner')
,('rush2112')
,('russia')
,('sammy')
,('samson')
,('sandra')
,('saturn')
,('scooby')
,('scooter')
,('scorpio')
,('scorpion')
,('scott')
,('secret')
,('sexsex')
,('sexy')
,('shannon')
,('shaved')
,('shit')
,('sierra')
,('silver')
,('skippy')
,('slayer')
,('slut')
,('smith')
,('smokey')
,('snoopy')
,('sophie')
,('spanky')
,('sparky')
,('spider')
,('squirt')
,('srinivas')
,('star')
,('stars')
,('startrek')
,('steelers')
,('steve')
,('steven')
,('sticky')
,('stupid')
,('success')
,('suckit')
,('super')
,('surfer')
,('swimming')
,('sydney')
,('teens')
,('tennis')
,('teresa')
,('test')
,('tester')
,('testing')
,('theman')
,('thunder')
,('thx1138')
,('tiffany')
,('tiger')
,('tigers')
,('time')
,('tits')
,('tomcat')
,('topgun')
,('toyota')
,('travis')
,('trouble')
,('tucker')
,('turtle')
,('united')
,('vagina')
,('victor')
,('victoria')
,('video')
,('viking')
,('viper')
,('voodoo')
,('voyager')
,('walter')
,('warrior')
,('white')
,('william')
,('willie')
,('wilson')
,('winner')
,('winston')
,('winter')
,('wizard')
,('wolf')
,('women')
,('xavier')
,('yamaha')
,('yankee')
,('yankees')
,('yellow')
,('young')) AS v([value])
OPTION (RECOMPILE); -- avoid saving this in plan cache

SET @RCount = @@ROWCOUNT;

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
	('12'),('21'),('123'),('1234'),('12345'),('123456'),('1234567'),('12345678'),('123456789'),('456'),('789'),('987'),('654'), --('789456'),
	('asd'),('qwe'),('qwer'),('qwert'),('qwerty'),('asdf'),('zxc'),('vbn'),('abc'),('abcd'),('uiop'),('zxcvbnm'),
	('1qaz'),('qaz'),('zaq'),('wsx'),('xsw'),('12qw'),('aszx'),('321'),('Aa'),('asdqwe'),('hello'),('qqww'),('1122'),
	('159'),('951'),('753'),('357'),('147'),('741'),('258'),('852'),('369'),('963'),('6655'),
	('a'),('b'),('0'),('1'),('2'),('3'),('!'),('#'),
	('10'),('20'),('00'),('fgh'),('jkl'),('asdfghjkl'),('ui'),('lol'),('love'),
	('fuckyou'),('iloveu'),('iloveyou'),('password'),('babygirl'),('football'),('jordan'),('michael'),('princess')
	) AS V(TxtWord)
), CTE_Level2
AS
(
SELECT CTE.generatedPwd
FROM NumbersCTE AS CTE
WHERE CTE.currLength = CTE.Pwdmaxlength
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

UNION ALL

-- Replicated text
SELECT REPLICATE(V.TxtChar, Occ.Occurences)
FROM (VALUES
('0',4,10),('1',4,10),('2',4,10),('3',4,10),('4',4,10),('5',4,10),('6',4,10),('7',4,10),('8',4,10),('9',4,10),('a',4,10)
,('123',4,4),('abc',4,4),('a1',1,4),('qwe',4,4),('12',1,4),('21',1,4),('13',1,4),('23',1,3)
,('bla',1,4),('25',2,4),('69',2,4),('100',2,4),('x',3,8),('z',1,10)
) AS V(TxtChar,MinOccurences,MaxOccurences)
INNER JOIN
(SELECT TOP (10) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Occurences FROM sys.all_columns) AS Occ
ON Occ.Occurences BETWEEN V.MinOccurences AND V.MaxOccurences

UNION ALL

-- Common permutations (singles)
SELECT TxtWord
FROM CommonPermutationWords

UNION ALL

-- Common permutations (doubles)
SELECT a.TxtWord + b.TxtWord
FROM CommonPermutationWords AS a
CROSS JOIN CommonPermutationWords AS b

UNION ALL

-- Common permutations (triples)
SELECT a.TxtWord + b.TxtWord + c.TxtWord
FROM CommonPermutationWords AS a
CROSS JOIN CommonPermutationWords AS b
CROSS JOIN CommonPermutationWords AS c
WHERE @BringThePain = 1
)
-- Generated passwords
INSERT INTO #pwd
SELECT LTRIM(RTRIM(generatedPwd))
FROM GeneratedPasswords
OPTION (RECOMPILE); -- avoid saving this in plan cache

SET @RCount = @RCount + @@ROWCOUNT;

PRINT N'Generated passwords: ' + CONVERT(nvarchar(MAX), @RCount)

IF @BringThePain = 1
BEGIN
	IF CONVERT(int, SERVERPROPERTY('EngineEdition')) NOT IN (1,2,4)
	OR CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 13
	CREATE CLUSTERED INDEX IXC ON #pwd (generatedPwd) WITH(DATA_COMPRESSION = PAGE);
	ELSE
	CREATE CLUSTERED INDEX IX ON #pwd (generatedPwd);
END

INSERT INTO #logins WITH (TABLOCKX)
(Deviation, LoginSID, LoginPrincipleId, LoginName, ServerRoles)
SELECT 
Deviation = dev.Deviation + CASE WHEN @OutputPasswords = 1 THEN N' (' + Pwd + N')' ELSE N'' END
, dev.[sid]
, dev.principal_id
, [LoginName]
, ServerRoles =
STUFF((
	SELECT N', ' + roles.name
	FROM sys.server_role_members AS srm
	INNER JOIN sys.server_principals AS roles ON srm.role_principal_id = roles.principal_id
	WHERE srm.member_principal_id = dev.principal_id
	FOR XML PATH('')
	), 1, 2, N'')
FROM
(
SELECT 'Empty Password' AS Deviation, s.sid, s.principal_id, RTRIM(name) AS [LoginName], '' AS Pwd
FROM sys.sql_logins AS s
WHERE is_disabled = 0
AND ([password_hash] IS NULL OR PWDCOMPARE('', [password_hash]) = 1)
AND name NOT IN ('MSCRMSqlClrLogin')
AND name NOT LIKE '##MS[_]%##'

UNION ALL

SELECT DISTINCT 'Login name is the same as password' AS Deviation, s.sid, s.principal_id, RTRIM(s.name) AS [Name] , u.usrname
FROM sys.sql_logins s
CROSS APPLY
(VALUES
(RTRIM(RTRIM(s.name))),
(REVERSE(RTRIM(RTRIM(s.name))))
) AS u(usrname)
WHERE s.is_disabled = 0
AND PWDCOMPARE(u.usrname, s.[password_hash]) = 1

UNION ALL

SELECT DISTINCT 'Weak Password' AS Deviation, s.sid, s.principal_id, RTRIM(s.name) AS [LoginName], d.generatedPwd
FROM #pwd d
INNER JOIN sys.sql_logins s ON PWDCOMPARE(d.generatedPwd, s.[password_hash]) = 1
WHERE s.is_disabled = 0
) AS dev
OPTION (RECOMPILE); -- avoid saving this in plan cache

IF OBJECT_ID('sys.server_permissions') IS NOT NULL
BEGIN
	UPDATE dev
		SET ServerPermissions =
		STUFF((
			SELECT N', ' + perm.state_desc + N' ' + perm.permission_name + N' ' + perm.class_desc
			FROM sys.server_permissions AS perm
			WHERE perm.grantee_principal_id = dev.LoginPrincipleId
			FOR XML PATH('')
			), 1, 2, N'')
	FROM #logins AS dev
	OPTION (RECOMPILE);
END

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
INNER JOIN sys.database_principals AS dp ON dp.sid = logins.LoginSID'

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

SET NOEXEC OFF;

SELECT Deviation
     , LoginName
     , ServerRoles
     , ServerPermissions
     , DBAccess
FROM #logins;
