#Set Parmeters:
  $access_token = (Get-AzAccessToken -ResourceUrl "https://database.windows.net").token

  $ServerInstance = 'adf-demo-eric.database.windows.net' # <============ Insert your Server Name
  $Database = 'master' 
  #$UserName = 'adf-demo-eric' # <============ Insert your Password
  #$Password = 'AzureDataFactory1!' # <============ Insert your User Name
  $Query = 'SELECT name FROM sys.databases'
  #$TargetTenants = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -UserName $UserName -Password $Password -Query $Query
  $TargetTenants = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -AccessToken $access_token -Query $Query
  $Query = 'EXEC sp_updatestats' # <============ Insert your command between the brackets

#Set a foreach loop for each of the Target Tenants:

  $TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $TargetTenant -AccessToken $access_token -Query $Query -Verbose
  }