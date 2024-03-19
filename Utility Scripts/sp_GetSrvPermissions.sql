IF OBJECT_ID('#sp_SrvPermissions') IS NOT NULL DROP PROCEDURE #sp_SrvPermissions
GO
/*********************************************************************************************
sp_SrvPermissions V6.1
Kenneth Fisher
 
http://www.sqlstudies.com
https://github.com/sqlstudent144/SQL-Server-Scripts/blob/master/sp_SrvPermissions.sql

This stored procedure returns 3 data sets.  The first dataset is the list of server
principals, the second is role membership, and the third is server level permissions.
    
The final 2 columns of each query are "Un-Do"/"Do" scripts.  For example removing a member
from a role or adding them to a role.  I am fairly confident in the role scripts, however, 
the scripts in the server principals query and server permissions query are works in
progress.  In particular certificates and keys are not scripted out.  Also while the scripts 
have worked flawlessly on the systems I've tested them on, these systems are fairly similar 
when it comes to security so I can't say that in a more complicated system there won't be 
the odd bug.
   
Notes on the create script for server principals:
1)  I have included a hashed version of the password and the sid.  This means that when run
    on another server the password and the sid will remain the same.  
2)  In SQL 2005 the create script on the server principals query DOES NOT WORK.  This is 
    because the conversion of the sid (in varbinary) to character doesn't appear to work
    as I expected in SQL 2005.  It works fine in SQL 2008 and above.  If you want to use
    this script in SQL 2005 you can change the CONVERTs in the principal script to
    master.sys.fn_varbintohexstr
   
Standard disclaimer: You use scripts off of the web at your own risk.  I fully expect this
     script to work without issue but I've been known to be wrong before.
    
Parameters:
    @Principal
        If NOT NULL then all three queries only pull for that server principal.  @Principal
        is a pattern check.  The queries check for any row where the passed in value exists.
        It uses the pattern '%' + @Principal + '%'
    @Role
        If NOT NULL then the roles query will pull members of the role.  If it is NOT NULL and
        @Principal is NULL then Server principal and permissions query will pull the principal 
        row for the role and the permissions for the role.  @Role is a pattern check.  The 
        queries check for any row where the passed in value exists.  It uses the pattern 
        '%' + @Role + '%'
    @Type
        If NOT NULL then all three queries will only pull principals of that type.  
        S = SQL login
        U = Windows login
        G = Windows group
        R = Server role
        C = Login mapped to a certificate
        K = Login mapped to an asymmetric key
    @DBName
        If NOT NULL then only return those principals and information about them where the 
        principal exists within the DB specified.
    @UseLikeSearch
        When this is set to 1 (the default) then the search parameters will use LIKE (and 
        %'s will be added around the @Principal and @Role parameters).  
        When set to 0 searchs will use =.
    @IncludeMSShipped
        When this is set to 1 (the default) then all principals will be included.  When set 
        to 0 the fixed server roles and SA and Public principals will be excluded.
    @DropTempTables
        When this is set to 1 (the default) the temp tables used are dropped.  If it's 0
        then the tempt ables are kept for references after the code has finished.
        The temp tables are:
            ##SrvPrincipals
            ##SrvRoles 
            ##SrvPermissions
    @Output
        What type of output is desired.
        Default - Either 'Default' or it doesn't match any of the allowed values then the SP
                    will return the standard 3 outputs.
        None - No output at all.  Usually used if you keeping the temp tables to do your own
                    reporting.
        CreateOnly - Only return the create scripts where they aren't NULL.
        DropOnly - Only return the drop scripts where they aren't NULL.
        ScriptsOnly - Return drop and create scripts where they aren't NULL.
        Report - Returns one output with one row per principal and a comma delimited list of
                    roles the principal is a member of and a comma delimited list of the 
                    individual permissions they have.
    @Print
        Defaults to 0, but if a 1 is passed in then the queries are not run but printed
        out instead.  This is primarily for debugging.
        
Data is ordered as follows
    1st result set: SrvPrincipal
    2nd result set: RoleName, LoginName if the parameter @Role is used else
                    LoginName, RoleName
    3rd result set: GranteeName 
  
*********************************************************************************************
-- V2.0
-- 8/18/2013 – Create a stub if the SP doesn’t exist, then always do an alter
-- 9/04/2013 – Change print option to show values of variables not the 
--             Variable names.
-- V3.0
-- 10/5/2013 - Added @Type parameter to pull only principals of a given type.
-- 10/20/2013 - Remove SID in CREATE LOGIN script from v2005 and lower since it requires
                a special function to convert from binary to varchar.
-- V4.0
-- 11/18/2013 - Corrected bug in the order of the parameters for sp_addsrvrolemember
                and sp_dropsrvrolemember, also added parameter names both.
-- 01/09/2014 - Added an ORDER BY to each of the result sets.  See above for details.
-- V5.0
-- 04/27/2014 - Add @DBName parameter
-- V5.5
-- 7/22/2014 - Changed strings to unicode
-- V6.0
-- 10/19/2014 - Add @UserLikeSearch and @IncludeMSShipped parameters. 
-- 03/25/2017 - Move SID towards the end of the first output so the more important 
--              columns are closer to the front.
-- 03/25/2017 - Add IF Exists to drop and create principal scripts
-- 03/25/2017 - Add @DropTempTables to keep the temp tables after the SP is run.
-- 03/26/2017 - Add @Output to allow different types of output.
-- V6.1
-- 06/13/2018 - Removed scripts for principal IDs under 100 (anecdotally the system IDs)
--            - Added SET NOCOUNT ON
-- 05/28/2019 - Add scripts & mappings for certificate & asymmetric key mapped principals.
--            - Start cleaning up the dynamic SQL a bit to make it easier to read.
--            - Fix SERVER ROLE scripts
--            - Add CHECK_POLICY and CHECK_EXPIRATION
--            - Add script support for disabled 
--            - Add script support for a single credential.  Will not support multiple credentials.
*********************************************************************************************/
CREATE PROCEDURE #sp_SrvPermissions 
(
    @Principal sysname = NULL, 
    @Role sysname = NULL, 
    @Type nvarchar(30) = NULL,
    @DBName sysname = NULL,
    @UseLikeSearch bit = 1,
    @IncludeMSShipped bit = 1,
    @DropTempTables bit = 1,
    @Output varchar(30) = 'Default',
    @Print bit = 0
)
AS

SET NOCOUNT ON
   
IF @DBName IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @DBName)
    BEGIN
        RAISERROR (N'%s is not a valid database name.',
        16,
        1,
        @DBName)
        RETURN
    END 

DECLARE @Collation nvarchar(50) 
SET @Collation = N' COLLATE ' + CAST(SERVERPROPERTY('Collation') AS nvarchar(50))
   
DECLARE @Version2005orLower bit
SELECT @Version2005orLower = CASE WHEN PARSENAME(CAST(SERVERPROPERTY('productversion') AS VARCHAR(20)),4) < 10 THEN 1
                            ELSE 0 END
   
DECLARE @sql nvarchar(max)
DECLARE @LikeOperator nvarchar(4)

IF @UseLikeSearch = 1
    SET @LikeOperator = N'LIKE'
ELSE 
    SET @LikeOperator = N'='

IF @UseLikeSearch = 1
BEGIN 
    IF LEN(ISNULL(@Principal,'')) > 0
        SET @Principal = N'%' + @Principal + N'%'
        
    IF LEN(ISNULL(@Role,'')) > 0
        SET @Role = N'%' + @Role+ N'%'
END

--=========================================================================
-- Server Principals
SET @sql = 
    N'SELECT Logins.principal_id AS SrvPrincipalId, Logins.name AS SrvPrincipal, Logins.type, Logins.type_desc, 
                Logins.is_disabled, Logins.default_database_name, Logins.default_language_name, 
                CASE sql_logins.is_policy_checked WHEN 1 THEN ''ON'' WHEN 0 THEN ''OFF'' END AS check_policy, 
                CASE sql_logins.is_expiration_checked WHEN 1 THEN ''ON'' WHEN 0 THEN ''OFF'' END AS check_expiration, 
                ISNULL(Cert.name,aKey.name) AS Cert_or_asymmetric_key,
                Logins.sid, 
       CASE WHEN Logins.principal_id < 100 THEN NULL ELSE 
            ''IF EXISTS (SELECT * FROM sys.server_principals WHERE name = '' + QuoteName(Logins.name,'''''''') + '') '' + 
               ''DROP '' + CASE Logins.type 
                   WHEN ''R'' THEN ''SERVER ROLE'' 
                   ELSE ''LOGIN'' END + 
               '' ''+QUOTENAME(Logins.name' + @Collation + ') END + '';'' AS DropScript, 
       CASE WHEN Logins.principal_id < 100 THEN NULL ELSE 
            ''IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = '' + QuoteName(Logins.name,'''''''') + '') '' + 
               ''CREATE '' + CASE Logins.type 
                   WHEN ''R'' THEN ''SERVER ROLE'' 
                   ELSE ''LOGIN'' END + 
               '' ''+QUOTENAME(Logins.name' + @Collation + ') END + 
               CASE WHEN Logins.type = (''S'') THEN '' WITH PASSWORD = '' + 
                    ISNULL(CONVERT(varchar(256), LOGINPROPERTY(Logins.name, ''PasswordHash''),1 ), ''0xchangeme'') + '' HASHED' +
                         CASE WHEN @Version2005orLower = 0 THEN N','' +  
                    '' SID = '' + CONVERT(varchar(85), Logins.sid, 1) ' 
                    ELSE N''' +  ' END + '
               WHEN Logins.type IN (''U'',''G'') THEN '' FROM WINDOWS ''  
               WHEN Logins.type = ''C'' THEN ISNULL('' FROM CERTIFICATE '' + QUOTENAME(Cert.name),'''')
               WHEN Logins.type = ''K'' THEN ISNULL('' FROM ASYMMETRIC KEY '' + QUOTENAME(aKey.name),'''')
               ELSE '''' END + 
               CASE WHEN Logins.type IN (''S'',''U'',''G'') THEN -- Note: Types, S, U and G are the only ones that have additional options.
                   CASE WHEN Logins.default_database_name IS NOT NULL OR Logins.default_language_name IS NOT NULL THEN
                        CASE WHEN Logins.Type = ''S'' THEN '','' ELSE '' WITH '' END
                   ELSE '''' END +
                   ISNULL('' DEFAULT_DATABASE = '' + QUOTENAME(Logins.default_database_name' + @Collation + N'), '''') + 
                   CASE WHEN Logins.default_database_name IS NOT NULL AND Logins.default_language_name IS NOT NULL THEN '','' ELSE '''' END + 
                   ISNULL('' DEFAULT_LANGUAGE = '' + QUOTENAME(Logins.default_language_name' + @Collation + N'), '''') +
                   CASE WHEN Logins.type = ''S'' THEN
                        ISNULL('', CHECK_EXPIRATION = '' + CASE WHEN sql_logins.is_expiration_checked = 1 THEN ''ON'' ELSE ''OFF'' END, '''') +
                        ISNULL('', CHECK_POLICY    = ''     + CASE WHEN sql_logins.is_policy_checked = 1 THEN ''ON'' ELSE ''OFF'' END, '''') +
                        ISNULL('', CREDENTIAL = ''         + QUOTENAME(Creds.name), '''')
                       ELSE '''' END
               ELSE '''' END +
               ''; '' +
               CASE WHEN Logins.is_disabled = 1 THEN ''ALTER LOGIN '' + QUOTENAME(Logins.name) + '' DISABLE; '' ELSE '''' END
           AS CreateScript 
    FROM sys.server_principals Logins 
    LEFT OUTER JOIN sys.certificates Cert
        ON Logins.sid = Cert.sid
    LEFT OUTER JOIN sys.asymmetric_keys aKey
        ON Logins.sid = aKey.sid
    LEFT OUTER JOIN sys.sql_logins
        ON Logins.sid = sql_logins.sid
    LEFT OUTER JOIN sys.server_principal_credentials LoginCreds
        ON Logins.principal_id = LoginCreds.principal_id
    LEFT OUTER JOIN sys.credentials Creds
        ON LoginCreds.credential_id = Creds.credential_id
    WHERE 1=1 '
   
IF LEN(ISNULL(@Principal,@Role)) > 0 
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Logins.name ' + @LikeOperator + N' ' + ISNULL(+QUOTENAME(@Principal,''''),QUOTENAME(@Role,'''')) 
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Logins.name ' + @LikeOperator + N' ISNULL(@Principal,@Role) '
   
IF LEN(@Type) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Logins.type ' + @LikeOperator + N' ' + QUOTENAME(@Type,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Logins.type ' + @LikeOperator + N' @Type'
 
IF @DBName IS NOT NULL
    SET @sql = @sql + NCHAR(13) + N'  AND Logins.SID IN (SELECT SID FROM [' + @DBName + N'].sys.database_principals 
                                                        WHERE type IN (''G'',''S'',''U'',''K'',''C''))'

IF @IncludeMSShipped = 0
    SET @sql = @sql + NCHAR(13) + N'  AND Logins.is_fixed_role = 0 ' + NCHAR(13) + 
                '  AND Logins.name NOT IN (''sa'',''public'') '
      
IF @Print = 1
    PRINT '-- Server Principals' + NCHAR(13) + @sql + NCHAR(13) + NCHAR(13)
ELSE
BEGIN
    IF object_id('tempdb..##SrvPrincipals') IS NOT NULL
        DROP TABLE ##SrvPrincipals

    -- Create temp table to store the data in
    CREATE TABLE ##SrvPrincipals (
        SrvPrincipalId int NULL,
        SrvPrincipal sysname NULL,
        type char(1) NULL,
        type_desc nchar(60) NULL,
        is_disabled bit NULL,
        default_database_name sysname NULL,
        default_language_name sysname NULL,
        [check_policy] char(3) NULL,
        [check_expiration] char(3) NULL,
        Cert_or_asymmetric_key sysname NULL,
        sid varbinary(85) NULL,
        DropScript nvarchar(max) NULL,
        CreateScript nvarchar(max) NULL
        )
    
    SET @sql =  N'INSERT INTO ##SrvPrincipals ' + NCHAR(13) + @sql

    EXEC sp_executesql @sql, N'@Principal sysname, @Role sysname, @Type varchar(30)', @Principal, @Role, @Type
END    
--=========================================================================
-- Server level roles
SET @sql = 
    N'SELECT Logins.principal_id AS LoginPrincipalId, Logins.name AS LoginName, Roles.name AS RoleName, 
       CASE WHEN Logins.principal_id < 100 THEN NULL ELSE 
       ''EXEC sp_dropsrvrolemember @loginame = ''+QUOTENAME(Logins.name' + @Collation + 
          ','''''''')+'', @rolename = ''+QUOTENAME(Roles.name' + @Collation + 
          ','''''''') + '';'' END AS DropScript, 
       CASE WHEN Logins.principal_id < 100 THEN NULL ELSE 
       ''EXEC sp_addsrvrolemember @loginame = ''+QUOTENAME(Logins.name' + @Collation + 
          ','''''''')+'', @rolename = ''+QUOTENAME(Roles.name' + @Collation + 
          ','''''''') + '';'' END AS AddScript 
    FROM sys.server_role_members RoleMembers 
    JOIN sys.server_principals Logins 
       ON RoleMembers.member_principal_id = Logins.principal_id 
    JOIN sys.server_principals Roles 
       ON RoleMembers.role_principal_id = Roles.principal_id 
    WHERE 1=1 '
   
IF LEN(ISNULL(@Principal,'')) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Logins.name ' + @LikeOperator + N' '+QUOTENAME(@Principal,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Logins.name ' + @LikeOperator + N' @Principal'
   
IF LEN(ISNULL(@Role,'')) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Roles.name ' + @LikeOperator + N' '+QUOTENAME(@Role,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Roles.name ' + @LikeOperator + N' @Role'
   
IF LEN(@Type) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Logins.type ' + @LikeOperator + N' ' + QUOTENAME(@Type,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Logins.type ' + @LikeOperator + N' @Type'
 
IF @DBName IS NOT NULL
    SET @sql = @sql + NCHAR(13) + N'  AND Logins.SID IN (SELECT SID FROM [' + @DBName + N'].sys.database_principals 
                                                        WHERE type IN (''G'',''S'',''U'',''K'',''C''))'
  
IF @IncludeMSShipped = 0
    SET @sql = @sql + NCHAR(13) + N'  AND Logins.is_fixed_role = 0 ' + NCHAR(13) + 
                '  AND Logins.name NOT IN (''sa'',''public'') '

IF @Print = 1
    PRINT '-- Server Role Members' + NCHAR(13) + @sql + NCHAR(13) + NCHAR(13)
ELSE
BEGIN
    IF object_id('tempdb..##SrvRoles') IS NOT NULL
        DROP TABLE ##SrvRoles

    -- Create temp table to store the data in
    CREATE TABLE ##SrvRoles (
        LoginPrincipalId int NULL,
        LoginName sysname NULL,
        RoleName sysname NULL,
        DropScript nvarchar(max) NULL,
        AddScript nvarchar(max) NULL
        )

    SET @sql =  'INSERT INTO ##SrvRoles ' + NCHAR(13) + @sql

    EXEC sp_executesql @sql, N'@Principal sysname, @Role sysname, @Type nvarchar(30)', @Principal, @Role, @Type
END
    
--=========================================================================
-- Server Permissions
SET @sql =
    N'SELECT Grantee.principal_id AS GranteePrincipalId, Grantee.name AS GranteeName, 
       Grantor.name AS GrantorName, Permission.class_desc, Permission.permission_name, 
       Permission.state_desc,  
       CASE WHEN Grantee.principal_id < 100 THEN NULL ELSE 
       ''REVOKE '' + 
           CASE WHEN Permission.class_desc = ''ENDPOINT'' THEN NULL 
           WHEN Permission.[state]  = ''W'' THEN ''GRANT OPTION FOR '' ELSE '''' END + 
           '' '' + Permission.permission_name' + @Collation + ' +  
           '' FROM '' + QUOTENAME(Grantee.name' + @Collation + ')  + ''; '' END AS RevokeScript, 
       CASE WHEN Grantee.principal_id < 100 THEN NULL ELSE 
       CASE WHEN Permission.class_desc = ''ENDPOINT'' THEN NULL 
           WHEN Permission.[state]  = ''W'' THEN ''GRANT'' ELSE Permission.state_desc' + @Collation + 
          ' END + 
           '' '' + Permission.permission_name' + @Collation + ' +  
           '' TO '' + QUOTENAME(Grantee.name' + @Collation + ')  + '' '' +  
           CASE WHEN Permission.[state]  = ''W'' THEN '' WITH GRANT OPTION '' ELSE '''' END +  
           '' AS ''+ QUOTENAME(Grantor.name' + @Collation + ') + '';'' END AS GrantScript 
    FROM sys.server_permissions Permission 
    JOIN sys.server_principals Grantee 
       ON Permission.grantee_principal_id = Grantee.principal_id 
    JOIN sys.server_principals Grantor 
       ON Permission.grantor_principal_id = Grantor.principal_id 
    WHERE 1=1 '
   
IF LEN(ISNULL(@Principal,@Role)) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Grantee.name ' + @LikeOperator + N' ' + ISNULL(+QUOTENAME(@Principal,''''),QUOTENAME(@Role,'''')) 
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Grantee.name ' + @LikeOperator + N' ISNULL(@Principal,@Role) '
   
IF LEN(@Type) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Grantee.type ' + @LikeOperator + N' ' + QUOTENAME(@Type,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Grantee.type ' + @LikeOperator + N' @Type'
  
IF @DBName IS NOT NULL
    SET @sql = @sql + NCHAR(13) + N' AND Grantee.SID IN (SELECT SID FROM [' + @DBName + N'].sys.database_principals 
                                    WHERE type IN (''G'',''S'',''U'',''K'',''C''))'
 
IF @IncludeMSShipped = 0
    SET @sql = @sql + NCHAR(13) + N'  AND Grantee.is_fixed_role = 0 ' + NCHAR(13) + 
                '  AND Grantee.name NOT IN (''sa'',''public'') '

IF @Print = 1
    PRINT '-- Server Permissions' + NCHAR(13) + @sql + NCHAR(13) + NCHAR(13)
ELSE
BEGIN
    IF object_id('tempdb..##SrvPermissions') IS NOT NULL
        DROP TABLE ##SrvPermissions

    -- Create temp table to store the data in
    CREATE TABLE ##SrvPermissions (
        GranteePrincipalId int NULL,
        GranteeName sysname NULL,
        GrantorName sysname NULL,
        class_desc nvarchar(60) NULL,
        permission_name nvarchar(128) NULL,
        state_desc nvarchar(60) NULL,
        RevokeScript nvarchar(max) NULL,
        GrantScript nvarchar(max) NULL
        )
    
    -- Add insert statement to @sql
    SET @sql = N'INSERT INTO ##SrvPermissions ' + NCHAR(13) + @sql

    EXEC sp_executesql @sql, N'@Principal sysname, @Role sysname, @Type nvarchar(30)', @Principal, @Role, @Type
END

IF @Print <> 1
BEGIN

    IF @Output = 'None'
        PRINT ''
    ELSE IF @Output = 'CreateOnly'
    BEGIN
        SELECT CreateScript FROM ##SrvPrincipals WHERE CreateScript IS NOT NULL
        UNION ALL
        SELECT AddScript FROM ##SrvRoles WHERE AddScript IS NOT NULL
        UNION ALL
        SELECT GrantScript FROM ##SrvPermissions WHERE GrantScript IS NOT NULL
    END 
    ELSE IF @Output = 'DropOnly' 
    BEGIN
        SELECT DropScript FROM ##SrvPrincipals WHERE DropScript IS NOT NULL
        UNION ALL
        SELECT DropScript FROM ##SrvRoles WHERE DropScript IS NOT NULL
        UNION ALL
        SELECT RevokeScript FROM ##SrvPermissions WHERE RevokeScript IS NOT NULL
    END
    ELSE IF @Output = 'ScriptOnly' 
    BEGIN
        SELECT DropScript, CreateScript FROM ##SrvPrincipals WHERE DropScript IS NOT NULL OR CreateScript IS NOT NULL
        UNION ALL
        SELECT DropScript, AddScript FROM ##SrvRoles WHERE DropScript IS NOT NULL OR AddScript IS NOT NULL
        UNION ALL
        SELECT RevokeScript, GrantScript FROM ##SrvPermissions WHERE RevokeScript IS NOT NULL OR GrantScript IS NOT NULL
    END
    ELSE IF @Output = 'Report'
    BEGIN
        SELECT SrvPrincipal, type, type_desc, is_disabled,
                STUFF((SELECT ', ' + ##SrvRoles.RoleName
                        FROM ##SrvRoles
                        WHERE ##SrvPrincipals.SrvPrincipalId = ##SrvRoles.LoginPrincipalId
                        ORDER BY ##SrvRoles.RoleName
                        FOR XML PATH(''),TYPE).value('.','VARCHAR(MAX)')
                    , 1, 2, '') AS RoleMembership,
                STUFF((SELECT ', ' + ##SrvPermissions.state_desc + ' ' + ##SrvPermissions.permission_name + ' ' +
                                CASE WHEN class_desc <> 'SERVER' THEN class_desc ELSE '' END
                        FROM (SELECT DISTINCT * FROM ##SrvPermissions) ##SrvPermissions
                        WHERE ##SrvPrincipals.SrvPrincipalId = ##SrvPermissions.GranteePrincipalId
                        ORDER BY ##SrvPermissions.state_desc, ##SrvPermissions.permission_name
                        FOR XML PATH(''),TYPE).value('.','VARCHAR(MAX)')
                    , 1, 2, '') AS DirectPermissions
        FROM ##SrvPrincipals
        ORDER BY SrvPrincipal
    END
    ELSE -- 'Default' or no match
    BEGIN
        SELECT SrvPrincipal, type, type_desc, is_disabled, default_database_name, 
                default_language_name, [check_policy], [check_expiration], Cert_or_asymmetric_key, 
                sid, DropScript, CreateScript 
        FROM ##SrvPrincipals ORDER BY SrvPrincipal
        IF LEN(@Role) > 0
            SELECT LoginName, RoleName, DropScript, AddScript FROM ##SrvRoles ORDER BY RoleName, LoginName
        ELSE
            SELECT LoginName, RoleName, DropScript, AddScript FROM ##SrvRoles ORDER BY LoginName, RoleName
        SELECT GranteeName, GrantorName, class_desc, permission_name, state_desc, RevokeScript, GrantScript 
        FROM ##SrvPermissions ORDER BY GranteeName
    END

    IF @DropTempTables = 1
    BEGIN
        DROP TABLE ##SrvPrincipals
        DROP TABLE ##SrvRoles
        DROP TABLE ##SrvPermissions
    END
END
GO

EXEC #sp_SrvPermissions