<#
Prerequisite:

Install the Azure Cosmos DB Desktop Data Migration Tool:

https://github.com/AzureCosmosDB/data-migration-desktop-tool


Additional resources:

https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-migrate-desktop-tool?tabs=azure-cli
https://github.com/AzureCosmosDB/data-migration-desktop-tool
https://www.youtube.com/watch?v=tofsZc_V_R8&ab_channel=MicrosoftDeveloper
https://www.youtube.com/watch?v=tIBd7D4nDP4&ab_channel=AzureCosmosDB
https://devblogs.microsoft.com/cosmosdb/new-desktop-data-migration-tool/
https://build5nines.com/azure-cosmos-db-desktop-data-migration-tool-v2-0/
#>
Param(
 ## Specify the relevant subscription Name.
 [string]$TenantName = "acme.com"

 ,[string]$SourceSubscriptionName = "Source Subscription Name"
 ,[string]$SourceResourceGroupName = "Source-Resource-Group"
 ,[string]$SourceCosmosDBAccountName = "source-cosmosdb-account"
 ,[string]$SourceCosmosDBAccountKey = "TheAccountKey=="

 ,[string]$TargetSubscriptionName = "Target Subscription Name"
 ,[string]$TargetResourceGroupName = "Target-Resource-Group"
 ,[string]$TargetCosmosDBAccountName = "target-cosmosdb-account"
 ,[string]$TargetCosmosDBAccountKey = "TheAccountKey=="

 ,[string]$DMTSettingsFilePath = "C:\windows-package\migrationsettings.json"
 ,[string]$DMTExecutablePath = "C:\windows-package\dmt.exe"
)
Process
{
#region initialization
function Get-TimeStamp {
    Param(
    [switch]$NoWrap,
    [switch]$Utc
    )
    $dt = Get-Date
    if ($Utc -eq $true) {
        $dt = $dt.ToUniversalTime()
    }
    $str = "{0:MM/dd/yy} {0:HH:mm:ss}" -f $dt

    if ($NoWrap -ne $true) {
        $str = "[$str]"
    }
    return $str
}

## Uninstall deprecated AzureRm modules
if (Get-Module -ListAvailable -Name "AzureRm*") {
    Write-Verbose "AzureRm module found. Uninstalling..."

    Get-Module -ListAvailable -Name "AzureRm*" | foreach {
        Write-Output "Uninstalling: $_"
        Remove-Module $_ -Force -Confirm:$false | Out-Null
        Uninstall-Module $_ -AllVersions -Force -Confirm:$false | Out-Null
    }
} 

## Install the modules that you need from the PowerShell Gallery

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

if (Get-PSRepository -Name "PSGallery") {
    Write-Verbose "$(Get-TimeStamp) PSGallery already registered"
} 
else {
    Write-Information "$(Get-TimeStamp) Registering PSGallery"
    Register-PSRepository -Default
}

## you can add or remove additional modules here as needed
$modules = @("Az.Accounts", "Az.CosmosDB")

foreach ($module in $modules) {
    if (Get-Module -ListAvailable -Name $module) {
        Write-Verbose "$module already installed"
    } 
    else {
        Write-Information "Installing $module"
        Install-Module $module -Force -SkipPublisherCheck -Scope CurrentUser -AllowClobber | Out-Null
        Import-Module $module -Force -PassThru -Scope Local | Out-Null
    }
}

## Log into Azure if you aren't already logged in. Unfortunately there
## appears to be a problem using regular MS accounts as credentials for
## Login-AzAccount so you have to go through the window & log in manually.
$needLogin = $true
Try 
{
    $content = Get-AzContext
    if ($content) 
    {
        $needLogin = ([string]::IsNullOrEmpty($content.Account))
    }
} 
Catch 
{
    if ($_ -like "*Connect-AzAccount to login*") 
    {
        $needLogin = $true
    } 
    else 
    {
        throw
    }
}

if ($needLogin)
{
    $connectionResult = Connect-AzAccount -Subscription $SourceSubscriptionName -Tenant $TenantName
}

## Switch to the correct directory and subscription

if ($content.Subscription.Name -ne $SourceSubscriptionName) {
    Connect-AzAccount -Subscription $SourceSubscriptionName -Tenant $TenantName | Out-Null
    $content = Get-AzContext

    if ($content.Subscription.Name -ne $SourceSubscriptionName)
    {
        Stop-PSFFunction -Message "Subscription not found" -Category InvalidArgument
    }
}

#endregion initialization


#region main


$MigrationSettings = '{
    "Source": "AzureTableAPI",
    "Sink": "AzureTableAPI",
    "SourceSettings": {
      "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=<storage-account-name>;AccountKey=<key>;EndpointSuffix=core.windows.net",
      "Table": "SourceTable1"
    },
    "SinkSettings": {
      "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=<storage-account-name>;AccountKey=<key>;EndpointSuffix=core.windows.net",
      "Table": "SinkTable1"
    }
}' | ConvertFrom-Json

$SourceConnectionString = "DefaultEndpointsProtocol=https;AccountName=$SourceCosmosDBAccountName;AccountKey=$SourceCosmosDBAccountKey;TableEndpoint=https://$SourceCosmosDBAccountName.table.cosmos.azure.com:443/;"
$TargetConnectionString = "DefaultEndpointsProtocol=https;AccountName=$TargetCosmosDBAccountName;AccountKey=$TargetCosmosDBAccountKey;TableEndpoint=https://$TargetCosmosDBAccountName.table.cosmos.azure.com:443/;"

$MigrationSettings.SourceSettings.ConnectionString = $SourceConnectionString
$MigrationSettings.SinkSettings.ConnectionString = $TargetConnectionString

$contextSource = Set-AzContext -SubscriptionName $SourceSubscriptionName -Tenant $TenantName


Write-Output "$(Get-TimeStamp) List all tables the source account $SourceCosmosDBAccountName"
$TablesList = Get-AzCosmosDBTable -ResourceGroupName $SourceResourceGroupName -AccountName $SourceCosmosDBAccountName

Write-Output "$(Get-TimeStamp) Found $($TablesList.Count) tables in source"



$contextTarget = Set-AzContext -SubscriptionName $TargetSubscriptionName -Tenant $TenantName

Write-Output "$(Get-TimeStamp) List all tables the target account $TargetCosmosDBAccountName"
$ExistingTablesList = Get-AzCosmosDBTable -ResourceGroupName $TargetResourceGroupName -AccountName $TargetCosmosDBAccountName

Write-Output "$(Get-TimeStamp) Found $($ExistingTablesList.Count) tables in target"

$ExistingTablesArray = @()
$ExistingTablesList | ForEach-Object {
    $ExistingTablesArray += $_.Name
}

$i = 0

$TablesList | ForEach-Object {
    $TableName = $_.Name
    $i += 1

    Write-Output "$(Get-TimeStamp) Table $i out of $($TablesList.Count): $TableName"

    Write-Progress -Activity "Copying tables from $SourceCosmosDBAccountName to $TargetCosmosDBAccountName" -Status "Copying Table $TableName ($i out of $($TablesList.Count))" -PercentComplete ($i / $TablesList.Count)


    if ($TableName -notin $ExistingTablesArray) {
        Write-Progress -Activity "Copying tables from $SourceCosmosDBAccountName to $TargetCosmosDBAccountName" -Status "Copying Table $TableName ($i out of $($TablesList.Count))" -PercentComplete ($i / $TablesList.Count) -CurrentOperation "Creating missing table"
        Write-Output "$(Get-TimeStamp) Creating missing table $TableName"

        $NewTable = New-AzCosmosDBTable -ResourceGroupName $TargetResourceGroupName -AccountName $TargetCosmosDBAccountName -Name $TableName
    }
    

    Write-Progress -Activity "Copying tables from $SourceCosmosDBAccountName to $TargetCosmosDBAccountName" -Status "Copying Table $TableName ($i out of $($TablesList.Count))" -PercentComplete ($i / $TablesList.Count) -CurrentOperation "Saving migration settings"
    Write-Output "$(Get-TimeStamp) Saving migration settings"

    $MigrationSettings.SourceSettings.Table = $TableName
    $MigrationSettings.SinkSettings.Table = $TableName

    $MigrationSettings | ConvertTo-Json | Out-File -FilePath $DMTSettingsFilePath -Force

    Write-Output "$(Get-TimeStamp) Executing migration for table $TableName..."
    Write-Progress -Activity "Copying tables from $SourceCosmosDBAccountName to $TargetCosmosDBAccountName" -Status "Copying Table $TableName ($i out of $($TablesList.Count))" -PercentComplete ($i / $TablesList.Count) -CurrentOperation "Executing migration"

    Start-Process -FilePath $DMTExecutablePath -ArgumentList @("--settings $DMTSettingsFilePath") -Wait -ErrorAction Stop -Verbose

    Write-Progress -Activity "Copying tables from $SourceCosmosDBAccountName to $TargetCosmosDBAccountName" -Status "Copying Table $TableName ($i out of $($TablesList.Count))" -PercentComplete ($i / $TablesList.Count) -CurrentOperation "Done"

}

Write-Progress -Activity "Copying tables from $SourceCosmosDBAccountName to $TargetCosmosDBAccountName" -Status "Migration completed for Table $TableName ($i out of $($TablesList.Count))" -PercentComplete 100 -Completed

Write-Output "$(Get-TimeStamp) Done"

#endregion main

}
