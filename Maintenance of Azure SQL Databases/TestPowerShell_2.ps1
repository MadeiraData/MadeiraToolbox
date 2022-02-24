#Set Parmeters:
  $ConnectionString = "Server=adf-demo-eric.database.windows.net;Database=master;Uid=adf-demo-eric;Pwd=AzureDataFactory1!;"
  $Query = 'SELECT name FROM sys.databases'
  $TargetTenants = Invoke-Sqlcmd -ConnectionString $ConnectionString -Query $Query
  $Query = 'EXEC sp_updatestats' # <============ Insert your command between the brackets

#Set a foreach loop for each of the Target Tenants:

  $TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ConnectionString $ConnectionString -Query $Query -Verbose
  }