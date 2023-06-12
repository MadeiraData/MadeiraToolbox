<#
.SYNOPSIS
Example Runbook for Azure Automation to Run Azure SQL Database Maintenance Tasks

.AUTHOR
Tracy Boggiano

.LINKS
Source: https://tracyboggiano.com/archive/2023/06/using-azure-automation-and-runbooks-to-run-azure-sql-database-maintenance-tasks/

.LINKS
Set up Azure Automation alerts: https://sqlitybi.com/how-to-add-monitoring-to-your-powershell-runbooks-if-they-fail/
#>

$errorActionPreference = "Stop"
Import-Module SqlServer

$Query = @"
EXECUTE dbo.IndexOptimize @Databases = 'ALL_DATABASES', @LogToTable = 'Y'
"@

$context = (Connect-AzAccount -Identity).Context

$Tenant = Get-AzTenant
$Subscription  = Get-AzSubscription -TenantID $Tenant.TenantId

ForEach ($sub in $Subscription) {
    $AzSqlServer = Get-AzSqlServer 

    if($AzSqlServer) {
        Foreach ($SQLServer in $AzSqlServer) {
            $SQLDatabase = Get-AzSqlDatabase -ServerName $SQLServer.ServerName -ResourceGroupName $SQLServer.ResourceGroupName | Where-Object { $_.DatabaseName -notin "master" }

            Foreach ($Database in $SQLDatabase) {
                $Token = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token

                Invoke-Sqlcmd -ServerInstance $SQLServer.FullyQualifiedDomainName -AccessToken $Token -Database $Database.DatabaseName -Query $Query -ConnectionTimeout 60 -Verbose
            }
        }
    }
}