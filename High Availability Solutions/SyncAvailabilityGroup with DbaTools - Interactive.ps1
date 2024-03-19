param
(
[string]$sourceSqlServer = $null,
[string]$targetSqlServer = $null,
[string]$agName = $null,
[pscredential]$sourceSqlCredential = $null,
[pscredential]$targetSqlCredential = $null,
[switch]$DisableJobsOnDestination = $null
)

#region input
$currentNT = "$([Environment]::UserDomainName)\$([Environment]::UserName)"

while ($sourceSqlServer -eq $null -or $sourceSqlServer -eq "")
{
    $sourceSqlServer = Read-Host -Prompt "Enter the SOURCE Sql Server instance address"
}

while ($targetSqlServer -eq $null -or $targetSqlServer -eq "")
{
    $targetSqlServer = Read-Host -Prompt "Enter the TARGET Sql Server instance address"
}

if ($agName -eq $null -or $agName -eq "")
{
    $agName = Read-Host -Prompt "Enter the AVAILABILITY GROUP name to check (leave empty to only copy all jobs and logins regardless of AG)"
}

if ($sourceSqlCredential -eq $null)
{
    $sourceUsername = Read-Host -Prompt "SOURCE username (leave empty to connect as '$currentNT' via Windows Authentication)"

    if ($sourceUsername -ne "")
    {
        $sourcePassword = Read-Host -Prompt "SOURCE password" -AsSecureString
        $sourceSqlCredential = New-Object System.Management.Automation.PSCredential ($sourceUsername, $sourcePassword)
    }
}

if ($targetSqlCredential -eq $null)
{
    $targetUsername = Read-Host -Prompt "TARGET username (leave empty to connect as '$currentNT' via Windows Authentication)"

    if ($targetUsername -ne "")
    {
        $targetPassword = Read-Host -Prompt "TARGET password" -AsSecureString
        $targetSqlCredential = New-Object System.Management.Automation.PSCredential ($targetUsername, $targetPassword)
    }
}

if ($DisableJobsOnDestination -eq $null)
{
    $disableJobs = Read-Host -Prompt "Do you want to DISABLE all JOBS on the TARGET? (y/n)"

    if ($disableJobs.ToLower().StartsWith('y')) {
        $DisableJobsOnDestination = $true
    } else {
        $DisableJobsOnDestination = $false
    }
}

#endregion input


#region init

## Install the modules that you need from the PowerShell Gallery

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

if (Get-PSRepository -Name "PSGallery") {
    Write-Verbose "PSGallery already registered"
} 
else {
    Write-Information "Registering PSGallery"
    Register-PSRepository -Default
}

## you can add or remove additional modules here as needed
$modules = @("dbatools")
        
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

#endregion init

#region connection

$sourceSQL = Connect-DbaInstance $sourceSqlServer -TrustServerCertificate -SqlCredential $sourceSqlCredential
$targetSQL = Connect-DbaInstance $targetSqlServer -TrustServerCertificate -SqlCredential $targetSqlCredential

#endregion connection


#region main

if ($agName -ne "")
{
    Sync-DbaAvailabilityGroup -Primary $sourceSQL -Secondary $targetSQL -AvailabilityGroup $agName -DisableJobOnDestination:$DisableJobsOnDestination
} else {
    Copy-DbaLogin -Source $sourceSQL -Destination $targetSQL
    Sync-DbaLoginPermission -Source $sourceSQL -Destination $targetSQL
    Copy-DbaAgentJob -Source $sourceSQL -Destination $targetSQL -DisableOnDestination:$DisableJobsOnDestination
    Copy-DbaLinkedServer -Source $sourceSQL -Destination $targetSQL
}

#endregion main
