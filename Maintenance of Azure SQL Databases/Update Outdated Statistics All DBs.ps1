param
(
 [string]$TargetServerInstance = 'adf-demo-eric.database.windows.net' # <============ Insert your Server Name containing the list of tenants
,[string]$TargetServerCredentials = 'adf-demo-sql_server' # <============ Insert your Azure Automation Credential Name
,[string]$TargetServerDatabase = 'master'
,[string]$QueryToGetTargetDatabaseNames = 'SELECT name FROM sys.databases WHERE database_id > 1'
,[string]$TSQLScriptURI = 'https://raw.githubusercontent.com/MadeiraData/MadeiraToolbox/master/Utility%20Scripts/Update%20Outdated%20Statistics%20All%20DBs%20(Parameterized).sql' # <=== Replace with the relevant github url (should be a raw file containing T-SQL)
)

# Get credentials object:
$Credentials = Get-AutomationPSCredential -Name $TargetServerCredentials

# Get list of database names
$TargetTenants = Invoke-Sqlcmd -ServerInstance $TargetServerInstance -Database $TargetServerDatabase -UserName $Credentials.UserName -Password $Credentials.GetNetworkCredential().Password -Query $QueryToGetTargetDatabaseNames

# Get the TSQL command to run on each tenant
$Query = Invoke-WebRequest -Uri $TSQLScriptURI -UseBasicParsing 
Write-Output "Running command:`n$Query"

# Loop for each of the Target Tenants:
$TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Write-Output "Executing in database $TargetTenant"
  Invoke-Sqlcmd -ServerInstance $TargetServerInstance -Database $TargetTenant -UserName $Credentials.UserName -Password $Credentials.GetNetworkCredential().Password -Query $Query -v MinimumTableRows = 200000 MinimumModCountr = 100000 MinimumDaysOld = 7 MaxDOP = 4 SampleRatePercent = NULL ExecuteRemediation = 1 TimeLimitMinutes = 10 -Verbose  
}
