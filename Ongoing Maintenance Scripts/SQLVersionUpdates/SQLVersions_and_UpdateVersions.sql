IF OBJECT_ID('[dbo].[SQLVersions]') IS NULL
BEGIN
CREATE TABLE [dbo].[SQLVersions](
	[Version] [varchar](6) NOT NULL,
	[BuildNumber] [nvarchar](255) NULL,
	[ReleaseDate] [datetime] NULL,
	[MajorVersionNumber] [int] NOT NULL,
	[MinorVersionNumber] [int] NOT NULL,
	[BuildVersionNumber] [int] NOT NULL, 
	[DownloadUrl] VARCHAR(200) NULL, 
	CONSTRAINT [PK_SQLVersions] PRIMARY KEY ([MajorVersionNumber], [MinorVersionNumber], [BuildVersionNumber])
) ON [PRIMARY]
END
GO
IF OBJECT_ID('[dbo].[UpdateVersions]') IS NULL EXEC (N'CREATE PROCEDURE [dbo].[UpdateVersions] AS RETURN')
GO
ALTER PROCEDURE [dbo].[UpdateVersions]
AS
SET TEXTSIZE 2147483647;
SET QUOTED_IDENTIFIER, NOCOUNT, ARITHABORT, XACT_ABORT ON;

DECLARE 
        @Url VARCHAR(8000),
        @xml VARCHAR(MAX),
        @resposta VARCHAR(MAX)
        
SET @Url = 'https://sqlserverbuilds.blogspot.com/'

IF OBJECT_ID('dbo.clr_http_request') IS NULL
BEGIN
	DECLARE @Fl_Ole_Automation_Ativado BIT = (SELECT CAST([value] AS BIT) FROM sys.configurations WHERE [name] = 'Ole Automation Procedures')
 
	IF (@Fl_Ole_Automation_Ativado = 0)
	BEGIN
		EXEC sp_configure 'show advanced options', 1;
		RECONFIGURE;
    
		EXEC sp_configure 'Ole Automation Procedures', 1;
		RECONFIGURE;    
	END
    
	DECLARE @obj INT
        
	EXEC sys.sp_OACreate 'MSXML2.ServerXMLHTTP', @obj OUT
	EXEC sys.sp_OAMethod @obj, 'open', NULL, 'GET', @Url, false
	EXEC sys.sp_OAMethod @obj, 'send'
 
	DECLARE @xml_versao_sql TABLE (Ds_Dados VARCHAR(MAX))
 
	INSERT INTO @xml_versao_sql(Ds_Dados)
	EXEC sys.sp_OAGetProperty @obj, 'responseText' --, @resposta OUT
	EXEC sys.sp_OADestroy @obj

	IF (@Fl_Ole_Automation_Ativado = 0)
	BEGIN
		EXEC sp_configure 'Ole Automation Procedures', 0;
		RECONFIGURE;
 
		EXEC sp_configure 'show advanced options', 0;
		RECONFIGURE;
	END
END
ELSE
BEGIN
	SELECT @resposta = dbo.clr_http_request('GET', @Url, NULL, NULL, 300000, 0, 0).value('/Response[1]/Body[1]', 'NVARCHAR(MAX)')
	OPTION(RECOMPILE);
END

DECLARE @Versao_SQL_Build VARCHAR(10)
		
--SET @Versao_SQL_Build = (CASE LEFT(CONVERT(VARCHAR, SERVERPROPERTY('ProductVersion')), 2)
--    WHEN '8.' THEN '2000'
--    WHEN '9.' THEN '2005'
--    WHEN '10' THEN (
--        CASE
--            WHEN LEFT(CONVERT(VARCHAR, SERVERPROPERTY('ProductVersion')), 4) = '10.5' THEN '2008 R2' 
--            WHEN LEFT(CONVERT(VARCHAR, SERVERPROPERTY('ProductVersion')), 4) = '10.0' THEN '2008' 
--        END)
--    WHEN '11' THEN '2012'
--    WHEN '12' THEN '2014'
--    WHEN '13' THEN '2016'
--    WHEN '14' THEN '2017'
--    WHEN '15' THEN '2019'
--    ELSE '2019'
--END)

SET @xml = @resposta COLLATE SQL_Latin1_General_CP1251_CS_AS

DECLARE @Atualizacoes_SQL_Server TABLE
(
	[Version_Major] VARCHAR(150),
	[Version_Prefix] VARCHAR(50),
	[Ultimo_Build] VARCHAR(100),
	[Ultimo_Build_SQLSERVR.EXE] VARCHAR(100),
	[Versao_Arquivo] VARCHAR(100),
	[Q] VARCHAR(100),
	[KB] VARCHAR(100),
	[Descricao_KB] VARCHAR(100),
	[Lancamento_KB] VARCHAR(100),
	[Download_Ultimo_Build] VARCHAR(100)
)
		
DECLARE
	@PosicaoInicialVersao INT,
	@PosicaoFinalVersao INT,
	@ExpressaoBuscar VARCHAR(100) ,
	@Version_Prefix VARCHAR(50),
	@RetornoTabela VARCHAR(MAX),
	@dadosXML XML

DECLARE Versions CURSOR
FOR
SELECT
	Versao_SQL_Build,
	BuildPrefix
FROM (VALUES
	 ('2000','8.')
	,('2005','9.')
	,('2008 R2','10.5')
	,('2008','10.0')
	,('2012','11')
	,('2014','12')
	,('2016','13')
	,('2017','14')
	,('2019','15')
	) AS v(Versao_SQL_Build, BuildPrefix)

OPEN Versions

FETCH NEXT FROM Versions INTO @Versao_SQL_Build, @Version_Prefix

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @ExpressaoBuscar = 'Microsoft SQL Server ' + @Versao_SQL_Build + ' Builds'
	SET @PosicaoInicialVersao = CHARINDEX(@ExpressaoBuscar, @xml) + LEN(@ExpressaoBuscar) + 6

	PRINT CONCAT(@ExpressaoBuscar, N' offset: ', @PosicaoInicialVersao, N', table location: ', CHARINDEX('<table', @xml, @PosicaoInicialVersao), N' to ', CHARINDEX('</table>', @xml, @PosicaoInicialVersao))

	SET @PosicaoInicialVersao = CHARINDEX('<table', @xml, @PosicaoInicialVersao)
	SET @PosicaoFinalVersao = CHARINDEX('</table>', @xml, @PosicaoInicialVersao)

	SET @RetornoTabela = SUBSTRING(@xml, @PosicaoInicialVersao, @PosicaoFinalVersao - @PosicaoInicialVersao + 8)


	-- Corrigindo classes sem aspas duplas ("")
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' border=1 cellpadding=4 cellspacing=0 bordercolor="#CCCCCC" style="border-collapse:collapse"', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' border=1 cellpadding=4 cellspacing=0 bordercolor="#CCCCCC" style="border-collapse:collapse;width:100%"', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' target=_blank rel=nofollow', ' target="_blank" rel="nofollow"')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' class=h', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' class=lsp', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' class=cu', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' class=sp', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' class=rtm', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' width=580', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' width=125', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' class=lcu', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' class=cve', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' class=lrtm', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' class=beta', '')
	SET @RetornoTabela = REPLACE(@RetornoTabela, ' title="Equivalent to the build number"', '')

	-- Corrigindo elementos não fechados corretamente
	SET @RetornoTabela = REPLACE(@RetornoTabela, '<th>', '</th><th>')
	SET @RetornoTabela = REPLACE(@RetornoTabela, '<tr></th>', '<tr>')
	SET @RetornoTabela = REPLACE(@RetornoTabela, '<th>Release Date</tr>', '<th>Release Date</th></tr>')

	SET @RetornoTabela = REPLACE(@RetornoTabela, '<td>', '</td><td>')
	SET @RetornoTabela = REPLACE(@RetornoTabela, '<tr></td>', '<tr>')

	SET @RetornoTabela = REPLACE(@RetornoTabela, '</tr>', '</td></tr>')
	SET @RetornoTabela = REPLACE(@RetornoTabela, '</th></td>', '</th>')
	SET @RetornoTabela = REPLACE(@RetornoTabela, '</td></td>', '</td>')

	-- Removendo elementos de entidades HTML
	SET @RetornoTabela = REPLACE(@RetornoTabela, '&nbsp;', ' ')
	SET @RetornoTabela = REPLACE(@RetornoTabela, '&kbln', '&amp;kbln')
	SET @RetornoTabela = REPLACE(@RetornoTabela, '<br>', '<br/>')

	--SELECT @RetornoTabela, @ExpressaoBuscar
	BEGIN TRY
		SET @dadosXML = CONVERT(XML, @RetornoTabela)
	END TRY
	BEGIN CATCH
		SELECT
		  @ExpressaoBuscar AS [@ExpressaoBuscar]
		, @RetornoTabela AS [@RetornoTabela]
		, @PosicaoFinalVersao AS [@PosicaoFinalVersao]
		, @PosicaoInicialVersao AS [@PosicaoInicialVersao]
		, @xml AS [@xml];
		PRINT N'Error at line ' + CONVERT(nvarchar(max),ERROR_LINE())
		PRINT ERROR_MESSAGE();
	END CATCH


	INSERT INTO @Atualizacoes_SQL_Server
	SELECT TOP 1 * 
	FROM
	(SELECT @Versao_SQL_Build AS Versao_SQL_Build, @Version_Prefix AS Version_Prefix,
	X.value('(td[1])[1]','varchar(100)') AS Ultimo_Build,
	X.value('(td[2])[1]','varchar(100)') AS [Ultimo_Build_SQLSERVR.EXE],
	X.value('(td[3])[1]','varchar(100)') AS Versao_Arquivo,
	X.value('(td[4])[1]','varchar(100)') AS [Q],
	X.value('(td[5])[1]','varchar(100)') AS KB,
	X.value('(td[6]/a)[1]','varchar(100)') AS Descricao_KB,
	X.value('(td[7])[1]','varchar(100)') AS Lancamento_KB,
	X.value('(td[6]/a/@href)[1]','varchar(100)') AS Download_Ultimo_Build
	FROM @dadosXML.nodes('//table/tr') AS T(X)
	WHERE X.query('.').exist('tr/td[1]/text()') = 1
	) AS a
	ORDER BY Lancamento_KB DESC


	FETCH NEXT FROM Versions INTO @Versao_SQL_Build, @Version_Prefix
END

CLOSE Versions
DEALLOCATE Versions

IF OBJECT_ID('tempdb..#tmpVersions') IS NOT NULL DROP TABLE #tmpVersions;

SELECT	[Version] = cast([Version] as varchar(6)),
	[BuildNumber] = cast([BuildNumber] as varchar(50)),
	[ReleaseDate] = cast([ReleaseDate] as datetime),
	[MajorVersionNumber] = cast([1] as int),
      	[MinorVersionNumber] = cast([2] as int),
      	[BuildVersionNumber] = cast([3] as int),
	[DownloadUrl] = Download_Ultimo_Build
INTO #tmpVersions
FROM (
SELECT 
	[Version] = REPLACE(Version_Major,'2008 R2','2008R2'),
	[BuildNumber] = Ultimo_Build,
	[ReleaseDate] = LEFT(Lancamento_KB,10),
	Download_Ultimo_Build,
	SPL.value,
	rowid = ROW_NUMBER() OVER(PARTITION BY Version_Major ORDER BY Lancamento_KB)
FROM @Atualizacoes_SQL_Server
CROSS APPLY STRING_SPLIT(Ultimo_Build,'.') AS SPL ) AS s
pivot
(MAX(value) FOR rowid in ([1],[2],[3])) p;


MERGE INTO [dbo].[SQLVersions] as trg
USING (SELECT * FROM #tmpVersions) as src
ON  trg.[MinorVersionNumber] = src.[MinorVersionNumber]
AND trg.[MajorVersionNumber] = src.[MajorVersionNumber]
AND trg.[BuildVersionNumber] = src.[BuildVersionNumber]
	
WHEN MATCHED THEN
	UPDATE SET [DownloadUrl] = src.[DownloadUrl]
WHEN NOT MATCHED THEN
INSERT 
([Version],[BuildNumber],[ReleaseDate],[MajorVersionNumber],[MinorVersionNumber],[BuildVersionNumber],[DownloadUrl])
VALUES
(src.[Version],src.[BuildNumber],src.[ReleaseDate],src.[MajorVersionNumber],src.[MinorVersionNumber],src.[BuildVersionNumber],src.[DownloadUrl]);


DROP TABLE #tmpVersions;
GO
