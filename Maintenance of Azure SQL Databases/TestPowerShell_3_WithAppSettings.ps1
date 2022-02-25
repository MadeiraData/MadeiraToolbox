#Set Parmeters:
  $ConnectionString = Get-AzFunctionAppSetting -Name ConnectionString -ResourceGroupName DBSmart-Development

  $Query = 'SELECT name FROM sys.databases'
  $TargetTenants = Invoke-Sqlcmd -ConnectionString $ConnectionString -Query $Query
  $Query = 'EXEC sp_updatestats' # <============ Insert your command between the brackets

#Set a foreach loop for each of the Target Tenants:

  $TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ConnectionString $ConnectionString -Query $Query -Verbose
  }


