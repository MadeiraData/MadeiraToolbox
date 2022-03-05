#Set Parmeters:

  $ServerInstance = 'adf-demo-eric.database.windows.net' # <============ Insert your Server Name
  $Database = 'master' 
  $UserName = 'adf-demo-eric' # <============ Insert your User Name
  $Password = 'AzureDataFactory1!' # <============ Insert your Password
  $Query = 'SELECT name FROM sys.databases WHERE database_id IN(6,7,8)'
  $TargetTenants = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -UserName $UserName -Password $Password -Query $Query

  $Query = 'TRUNCATE TABLE dbo.Institute' # <============ Insert your command between the brackets

#Set a foreach loop for each of the Target Tenants:

  $TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $TargetTenant -UserName $UserName -Password $Password -Query $Query -Verbose
  Write-Host "Executing" $Query "in database" $TargetTenant
  }

 