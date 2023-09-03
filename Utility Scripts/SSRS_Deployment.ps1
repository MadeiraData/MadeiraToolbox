Param
(
[string] $SourceFolder = "C:\SSRS\My-Reports",
[string] $TargetReportServerUri = "http://localhost:8081/ReportServer",
[PSCredential] $Credential,
[switch] $CustomAuthentication,
[string] $ApiVersion,
[string] $TargetFolder = "/My-Reports/Sample-Reports",
[string] $OverrideDataSourcePathForAll, #= "/My-Reports/Data Sources/ProdDS",
[string] $logFileFolderPath = "C:\SSRS_deployment_log",
[string] $logFilePrefix = "ssrs_deploy_",
[string] $logFileDateFormat = "yyyyMMdd_HHmmss",
[int] $logFileRetentionDays = 30
)
Process {
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

if ($logFileFolderPath -ne "")
{
    if (!(Test-Path -PathType Container -Path $logFileFolderPath)) {
        Write-Output "$(Get-TimeStamp) Creating directory $logFileFolderPath" | Out-Null
        New-Item -ItemType Directory -Force -Path $logFileFolderPath | Out-Null
    } else {
        $DatetoDelete = $(Get-Date).AddDays(-$logFileRetentionDays)
        Get-ChildItem $logFileFolderPath | Where-Object { $_.Name -like "*$logFilePrefix*" -and $_.LastWriteTime -lt $DatetoDelete } | Remove-Item | Out-Null
    }
    
    $logFilePath = $logFileFolderPath + "\$logFilePrefix" + (Get-Date -Format $logFileDateFormat) + ".LOG"

    # attempt to start the transcript log, but don't fail the script if unsuccessful:
    try 
    {
        Start-Transcript -Path $logFilePath -Append
    }
    catch [Exception]
    {
        Write-Warning "$(Get-TimeStamp) Unable to start Transcript: $($_.Exception.Message)"
        $logFileFolderPath = ""
    }
}
#endregion initialization


#region install-modules

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

if (Get-PSRepository -Name "PSGallery") {
    Write-Verbose "$(Get-TimeStamp) PSGallery already registered"
} 
else {
    Write-Information "$(Get-TimeStamp) Registering PSGallery"
    Register-PSRepository -Default
}

if (Get-Module -ListAvailable -Name PowerShellGet) {
    Write-Verbose "$(Get-TimeStamp) PowerShellGet already installed"
} 
else {
    Write-Information "$(Get-TimeStamp) Installing PowerShellGet"
    Install-Module PowerShellGet -RequiredVersion 2.2.4 -Force -SkipPublisherCheck -Scope CurrentUser -ErrorAction Stop -AllowClobber | Out-Null
    Import-Module PowerShellGet -Force -Scope Local | Out-Null
}

if ((Get-PSRepository -Name "PSGallery").Trusted) {
    Write-Verbose "$(Get-TimeStamp) PSGallery already Trusted"
} else {
    Write-Information "$(Get-TimeStamp) Marking PSGallery as Trusted..."
    Set-PSRepository -Name "PSGallery" -InstallationPolicy Trusted
}

# replace the array below with any modules that your script depends on.
# you can remove this region if your script doesn't need importing any modules.
$modules = @("ReportingServicesTools")
        
foreach ($module in $modules) {
    if (Get-Module -ListAvailable -Name $module) {
        Write-Verbose "$(Get-TimeStamp) $module already installed"
    } 
    else {
        Write-Information "$(Get-TimeStamp) Installing $module"
        Install-Module $module -Force -SkipPublisherCheck -Scope CurrentUser -ErrorAction Stop | Out-Null
        Import-Module $module -Force -Scope Local | Out-Null
    }
}

#Write-Output "Requesting RSTools..."
#Invoke-Expression (Invoke-WebRequest https://aka.ms/rstools)

#endregion install-modules


#region main

$ErrorActionPreference = "Stop"


if ($SourceFolder -eq "" -or $SourceFolder -eq $null) {
    $SourceFolder = $(Get-Location).Path + "\"
}

if ($TargetFolder -eq "" -or $TargetFolder -eq $null) {
    $TargetFolder = "/"
}

if (!$SourceFolder.EndsWith("\"))
{
    $SourceFolder = $SourceFolder + "\"
}


Write-Output "====================================================================================="
Write-Output "                             Deploying SSRS Reports"
Write-Output "Source Folder: $SourceFolder"
Write-Output "Target Server: $TargetReportServerUri"
Write-Output "Target Folder: $TargetFolder"


$ProxyParams = @{
    ReportServerUri = $TargetReportServerUri
    CustomAuthentication = $CustomAuthentication
}

if ($Credential -ne $null) {
    Write-Output "Username: $($Credential.UserName)"
    $ProxyParams.Credential = $Credential
}

if ($ApiVersion -ne $null) {
    Write-Output "ApiVersion: $ApiVersion"
    $ProxyParams.ApiVersion = $ApiVersion
}

Write-Output "====================================================================================="

$Proxy = New-RsWebServiceProxy @ProxyParams


if ($TargetFolder -ne "/") {

  if ($TargetFolder.StartsWith("/")) {
      $TargetFolder = $TargetFolder.Remove(0,1)
  }
  
  Write-Output "$(Get-TimeStamp) Creating Folder: $TargetFolder"
  New-RsFolder -Proxy $Proxy -Path / -Name $TargetFolder -Verbose -ErrorAction SilentlyContinue
}

if (!$TargetFolder.StartsWith("/")) {
    $TargetFolder = $TargetFolder.Insert(0, "/")
}

Write-Output "$(Get-TimeStamp) Deploying Data Source files from: $SourceFolder"
DIR $SourceFolder -Filter *.rds | % { $_.FullName } |
    Write-RsCatalogItem -Proxy $Proxy -Destination $TargetFolder -Verbose -Overwrite

Write-Output "$(Get-TimeStamp) Deploying Data Set files from: $SourceFolder"
DIR $SourceFolder -Filter *.rsd | % { $_.FullName } |
    Write-RsCatalogItem -Proxy $Proxy -Destination $TargetFolder -Verbose -Overwrite

Write-Output "$(Get-TimeStamp) Deploying Report Definition files from: $SourceFolder"
DIR $SourceFolder -Filter *.rdl | % { $_.FullName } |
    Write-RsCatalogItem -Proxy $Proxy -Destination $TargetFolder -Verbose -Overwrite
    
if ($OverrideDataSourcePathForAll -ne $null -and $OverrideDataSourcePathForAll -ne "") {
    Write-Output "$(Get-TimeStamp) Fixing Data Source references to: $OverrideDataSourcePathForAll"

    Get-RsFolderContent -Proxy $Proxy -RsFolder $TargetFolder | ForEach {
        $CurrReport = $_
        Get-RsItemReference -Proxy $Proxy -Path $CurrReport.Path | Where ReferenceType -eq "DataSource" | ForEach {
            $CurrReference = $_

            if ($CurrReference.Reference -ne $OverrideDataSourcePathForAll) {
                Write-Output "$(Get-TimeStamp) UPDATING: Data Source $($CurrReference.Name) in report $($CurrReport.Path)"
                Set-RsDataSourceReference -Proxy $Proxy -Path $CurrReport.Path -DataSourceName $CurrReference.Name -DataSourcePath $OverrideDataSourcePathForAll
            } else {
                Write-Output "$(Get-TimeStamp) Data Source $($CurrReference.Name) in report $($CurrReport.Path) already set correctly."
            }
        }
    }
}

#endregion main


#region finalization
if ($logFileFolderPath -ne "") { Stop-Transcript }
#endregion finalization
}