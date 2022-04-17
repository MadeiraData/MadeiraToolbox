#Set Parmeters:

  $ServerInstance = 'yourserver.database.windows.net' # <============ Insert your Server Name
  $Database = 'master' 
  $UserName = 'yourusername' # <============ Insert your User Name
  $Password = 'Yourp@ssword123' # <============ Insert your Password
  $Query = 'SELECT name FROM sys.databases WHERE database_id NOT IN(1,5)'
  $TargetTenants = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -UserName $UserName -Password $Password -Query $Query
  
  $Query = 'DBCC CHECKDB WITH NO_INFOMSGS' # <============ Insert your command between the brackets

#Set a foreach loop for each of the Target Tenants:

$TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $TargetTenant -UserName $UserName -Password $Password -InputFile $InputFile -Verbose
}