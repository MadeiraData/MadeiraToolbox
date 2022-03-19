#Set Parmeters:

  $ServerInstance = 'yourserver.database.windows.net' # <============ Insert your Server Name
  $Database = 'master' 
  $UserName = 'yourusername' # <============ Insert your Password
  $Password = 'Yourp@ssword123' # <============ Insert your User Name
  $Query = 'SELECT name FROM sys.databases WHERE database_id NOT IN(1,5)'
  $TargetTenants = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -UserName $UserName -Password $Password -Query $Query

  $InputFile = '' # <============ Insert your file path between the brackets

#Set a foreach loop for each of the Target Tenants:

  $TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $TargetTenant -UserName $UserName -Password $Password -InputFile $InputFile -Verbose
  }
