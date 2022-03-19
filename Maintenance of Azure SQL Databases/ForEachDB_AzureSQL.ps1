#Set Parmeters:

  $ServerInstance = 'yourserver.database.windows.net' # <============ Insert your Server Name
  $Database = 'master' 
  $UserName = 'yourusername' # <============ Insert your User Name
  $Password = 'Yourp@ssword123' # <============ Insert your Password
  $Query = 'SELECT name FROM sys.databases WHERE database_id > 1'
  $TargetTenants = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -UserName $UserName -Password $Password -Query $Query

  $Query = 'CREATE TABLE dbo.TestTable' # <============ Insert your command between the brackets

#Set a foreach loop for each of the Target Tenants:

  $TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $TargetTenant -UserName $UserName -Password $Password -Query $Query -Verbose
  Write-Host "Executing" $Query "in database" $TargetTenant
  }

 