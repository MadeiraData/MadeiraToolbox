using namespace System.Net

# Input bindings are passed in via param block.
param($Request, $TriggerMetadata)

# Write to the Azure Functions log stream.
Write-Host "PowerShell HTTP trigger function processed a request."

#Set Variables:

$UserName = $Request.Body.UserName # <============ Will be defined in the ADF settings / body
$Password = $Request.Body.Password # <============ Will be defined in the ADF settings / body

$ServerInstance = 'adf-demo-eric.database.windows.net' # <============ Insert your Server Name
$Database = 'master' 
$Query = 'SELECT name FROM sys.databases WHERE database_id > 1'
$TargetTenants = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -UserName $UserName -Password $Password -Query $Query

# Replace the -Uri param value with the relevant github url (should be a file containing T-SQL)
$Query = Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/MadeiraData/MadeiraToolbox/master/Maintenance%20of%20Azure%20SQL%20Databases/CreateTableTest678.sql' -UseBasicParsing 

#Set a foreach loop for each of the Target Tenants:

  $TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $TargetTenant -UserName $UserName -Password $Password -Query $Query -Verbose
  Write-Host "$Query is being executed in $TargetTenant database"
  }




