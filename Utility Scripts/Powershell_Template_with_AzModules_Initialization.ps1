Param(
 ## Specify the relevant subscription Name.
 [Parameter(Mandatory=$false,
 HelpMessage="Enter the Name of the relevant Subscription")]
 [string]
 $SubscriptionName = "Visual Studio MPN"
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
$modules = @("Az.Accounts", "Az.Compute", "Az.Sql")
        
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
    Connect-AzAccount -Subscription $SubscriptionName | Out-Null
}

## Switch to the correct directory and subscription

Get-AzSubscription | Where-Object {$_.Name -eq $SubscriptionName} | ForEach-Object {
    Write-Output "Switching to subscription '$($_.Name)' in TenantId '$($_.TenantId)'"
    $SubscriptionId = $_.Id
    Connect-AzAccount -Subscription $SubscriptionName -Tenant $_.TenantId | Out-Null
}

if ($SubscriptionId -eq "" -or $SubscriptionId -eq $null)
{
    Stop-PSFFunction -Message "No suitable subscription found" -Category InvalidArgument
}


#endregion initialization


#region main


## TODO: Add your code here


#endregion main

}