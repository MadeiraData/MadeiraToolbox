USE [SentryOne]
GO
PRINT N'View dbo.SQLBuildVersionCheck...'
GO
/*
Please use the SentryOne Condition variant (SQL Server > Repository Query):

SELECT MessageText, DaysSinceRelease FROM dbo.SQLBuildVersionCheck
WHERE DeviceID = @ComputerID
*/
CREATE OR ALTER VIEW dbo.SQLBuildVersionCheck
AS
SELECT MessageText = N'SQL Server ' + QUOTENAME(S.[ObjectName] COLLATE database_default) + N' (version '+ Al.[version] COLLATE database_default +') has an old Build version <b><pre>'
  + S.[version] COLLATE database_default +'</pre></b> instead of the most current Build version of <b><pre>' + al.BuildNumber COLLATE database_default + N'</pre></b>.<br/>Download link: ' 
  + N'<a href="' + ISNULL(al.[DownloadUrl] COLLATE database_default + N'" target="_blank"', N'#') + N'>' + ISNULL(al.[DownloadUrl] COLLATE database_default, N'(unavailable)') 
  + N'</a><br/>[Release Date] ' + ISNULL(CONVERT(nvarchar(10), Al.ReleaseDate, 121), N'N/A')
, DaysSinceRelease = DATEDIFF(day, Al.ReleaseDate, GETDATE())
, S.DeviceID
FROM [SentryOne].dbo.vwSqlServer S
INNER JOIN [SentryOne].dbo.[vwServerList] Sl ON S.ObjectID = Sl.ObjectID
CROSS APPLY (
 SELECT TOP (1) Al.*
 FROM [dbo].[SQLVersions] AS Al
 WHERE  Al.MajorVersionNumber = S.MajorVersionNumber
 and Al.MinorVersionNumber = S.MinorVersionNumber
 and S.BuildVersionNumber < Al.BuildVersionNumber 
 ORDER BY Al.BuildNumber DESC
) Al 
WHERE 
Al.ReleaseDate BETWEEN DATEADD(year,-1,GETDATE()) AND DATEADD(month,-1,GETDATE())
--AND S.DeviceID = @ComputerID
GO
GRANT SELECT ON dbo.SQLBuildVersionCheck TO allow_all
GO
