#Set Parmeters:

  $ServerInstance = 'adf-demo-eric.database.windows.net' # <============ Insert your Server Name
  $Database = 'master' 
  $UserName = 'adf-demo-eric' # <============ Insert your User Name
  $Password = 'AzureDataFactory1!' # <============ Insert your Password
  $Query = 'SELECT name FROM sys.databases WHERE database_id > 1'
  $TargetTenants = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -UserName $UserName -Password $Password -Query $Query

  $Query = 'create or alter proc dbo.UpdateStats
  as
  begin
  exec sp_updatestats
  end
  ' # <============ Insert your command between the brackets

#Set a foreach loop for each of the Target Tenants:

  $TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $TargetTenant -UserName $UserName -Password $Password -Query $Query -Verbose
  Write-Host "$Query is being executed in $TargetTenant database"
  }

  #Write-Host "sp $Query is being executed in $TargetTenant database"