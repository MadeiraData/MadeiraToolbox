#Set Variables:

$UserName = 'MySQLUsername' # <============ Will be defined in the ADF settings / body
$Password = 'MyTopSecretPassword' # <============ Will be defined in the ADF settings / body

$ServerInstance = 'my-sql-server.database.windows.net' # <============ Insert your Server Name
$Database = 'master' 
$Query = 'SELECT name FROM sys.databases WHERE database_id > 1'
$TargetTenants = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -UserName $UserName -Password $Password -Query $Query

# Replace the -Uri param value with the relevant github url (should be a file containing T-SQL)
$Query = Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/MadeiraData/MadeiraToolbox/master/Health%20Check%20Scripts/Untrusted_Foreign_Keys_all_dbs.sql' -UseBasicParsing 

#Set a foreach loop for each of the Target Tenants:

$TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $TargetTenant -UserName $UserName -Password $Password -Query $Query -Variable "RunRemediation=Yes" -Verbose
  Write-Host "$Query is being executed in $TargetTenant database"
}
