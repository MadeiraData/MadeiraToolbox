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

$Query = 'create table dbo.Test567 (Id int)' # <============ Insert your command between the brackets

#Set a foreach loop for each of the Target Tenants:

  $TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $TargetTenant -UserName $UserName -Password $Password -Query $Query -Verbose
  Write-Host "$Query is being executed in $TargetTenant database"
  }