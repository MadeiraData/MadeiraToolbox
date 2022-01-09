SET NOCOUNT ON;

SELECT @@SERVERNAME AS [server_name]
, grantee_name = 'public'
, perm.class_desc
, perm.major_id
, perm.minor_id
, perm.grantor_principal_id
, grantor_name = SUSER_NAME(perm.grantor_principal_id)
, perm.type
, perm.permission_name
, perm.state
, perm.state_desc
FROM sys.server_permissions AS perm WITH(NOLOCK)
WHERE perm.grantee_principal_id = SUSER_ID('public')
AND perm.[type] NOT IN ('VWDB','CO')

