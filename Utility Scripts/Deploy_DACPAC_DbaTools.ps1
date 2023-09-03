# when creating a scheduled task to run such scripts, use the following structure example:
# powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\Madeira\my_powershell_script.ps1"

<#
.DESCRIPTION
This script publishes the dacpac to the sql server instance.

.LINK
Based on: https://github.com/sanderstad/Azure-Devops-Duet
#>
Param
(
[string]$SqlInstance,
[PSCredential]$SqlCredential,
[string]$SqlUserName,
[string]$SqlPassword,
[string]$Database,
[string]$DacPacFilePath,
[string]$PublishXmlFile,
[switch]$EnableException,
[string]$OutputFolderPath,
[switch]$GenerateDeploymentReport,
[switch]$ScriptOnly,
[string]$logFileFolderPath = "C:\Madeira\log",
[string]$logFilePrefix = "dacpac_deploy_",
[string]$logFileDateFormat = "yyyyMMdd_HHmmss",
[int]$logFileRetentionDays = 30
)
Process {
#region initialization
if ($logFileFolderPath -ne "")
{
    if (!(Test-Path -PathType Container -Path $logFileFolderPath)) {
        Write-Output "Creating directory $logFileFolderPath" | Out-Null
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
        Write-Warning "Unable to start Transcript: $($_.Exception.Message)"
        $logFileFolderPath = ""
    }
}
#endregion initialization

#region validations

if (-not $SqlInstance) {
    Write-Error -Message "Please enter a SQL Server instance" -Category InvalidArgument -ErrorAction Stop
    return
}

if (-not $SqlCredential -and ($SqlUserName -and $SqlPassword)) {
    $password = ConvertTo-SecureString $SqlPassword -AsPlainText -Force;
    $SqlCredential = New-Object System.Management.Automation.PSCredential($SqlUserName, $password);
}

if (-not $SqlCredential) {
    Write-Output "Using Windows Authentication as $([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)"
}

if (-not $Database) {
    Write-Error -Message "Please enter a database" -Category InvalidArgument -ErrorAction Stop
    return
}

if (-not $DacPacFilePath) {
    Write-Error -Message "Please enter a DACPAC file" -Category InvalidArgument -ErrorAction Stop
    return
}
elseif (-not (Test-Path -Path $DacPacFilePath)) {
    Write-Error -Message "Could not find DACPAC file $DacPacFilePath" -Category InvalidArgument -ErrorAction Stop
    return
}

if (-not $PublishXmlFile) {
    Write-Error -Message "Please enter a publish profile file" -Category InvalidArgument -ErrorAction Stop
    return
}
elseif (-not (Test-Path -Path $PublishXmlFile)) {
    Write-Error -Message "Could not find publish profile file $PublishXmlFile" -Category InvalidArgument -ErrorAction Stop
    return
}


#endregion validations

#region install-modules

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

if (Get-PSRepository -Name "PSGallery") {
    Write-Verbose "PSGallery already registered"
} 
else {
    Write-Information "Registering PSGallery"
    Register-PSRepository -Default
}

# replace the array below with any modules that your script depends on.
# you can remove this region if your script doesn't need importing any modules.
$modules = @("PSFramework", "PSModuleDevelopment", "dbatools")
        
foreach ($module in $modules) {
    if (Get-Module -ListAvailable -Name $module) {
        Write-Verbose "$module already installed"
    } 
    else {
        Write-Information "Installing $module"
        Install-Module $module -Force -SkipPublisherCheck -Scope CurrentUser -ErrorAction Stop -AllowClobber | Out-Null
        Import-Module $module -Force -Scope Local -PassThru | Out-Null
    }
}
#endregion install-modules


#region main


try {
    $server = Connect-DbaInstance -SqlInstance $SqlInstance -SqlCredential $SqlCredential
    Write-PSFMessage -Level Important -Message "Connected to $($server.Name)"
}
catch {
    Write-Error -Message "Could not connect to $SqlInstance - $($_)" -Category ConnectionError -ErrorAction Stop
}


# Publish the DACPAC file

$paramSplat = @{
    SqlInstance   = $SqlInstance
    Database      = $Database
    Path          = $DacPacFilePath
    PublishXml    = $PublishXmlFile
}

if ($SqlCredential) {
    $paramSplat["SqlCredential"] = $SqlCredential
}

if ($OutputFolderPath) {
    if (!(Test-Path -PathType Container -Path $OutputFolderPath)) {
        New-Item -ItemType Directory -Path $OutputFolderPath
    }

    $paramSplat["OutputPath"] = $OutputFolderPath
}

if ($EnableException) {
    $paramSplat["EnableException"] = $true
}

if ($GenerateDeploymentReport) {
    $paramSplat["GenerateDeploymentReport"] = $true
}

if ($ScriptOnly) {
    $paramSplat["ScriptOnly"] = $true
}

try {
    Write-PSFMessage -Level Important -Message "Publishing DacPac to database $($paramSplat.Database) in server $($paramSplat.SqlInstance)"
    $DacPacResults = Publish-DbaDacPackage @paramSplat -Verbose

    # use these as necessary:
    $DacPacResults
    #$DacPacResults.Result
    #$DacPacResults.DatabaseScriptPath
    #$DacPacResults.DeploymentReport
}
catch {
    Write-Error -Message "Could not publish DacPac to database $($paramSplat.Database) in server $($paramSplat.SqlInstance) - $($_)" -Category InvalidResult -ErrorAction Stop
}


#endregion main


#region finalization
if ($logFileFolderPath -ne "") { Stop-Transcript }
#endregion finalization
}
