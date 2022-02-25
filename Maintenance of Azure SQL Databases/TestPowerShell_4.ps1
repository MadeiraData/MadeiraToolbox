#Set Parmeters:

  $ServerInstance = 'adf-demo-eric.database.windows.net' # <============ Insert your Server Name
  $Database = 'master' 
  $UserName = 'adf-demo-eric' # <============ Insert your Password
  $Password = 'AzureDataFactory1!' # <============ Insert your User Name
  $Query = 'SELECT name FROM sys.databases'
  $TargetTenants = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -UserName $UserName -Password $Password -Query $Query

  $Query = 'DROP TABLE IF EXISTS dbo.Test
            CREATE TABLE dbo.Test
	        (
		    Id INT NOT NULL
	        )
            GO' # <============ Insert your command between the brackets

#Set a foreach loop for each of the Target Tenants:

  $TargetTenants | ForEach-Object{
  $TargetTenant = $_.name
  Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $TargetTenant -UserName $UserName -Password $Password -Query $Query -Verbose
  }
