param
(
[string[]]$TargetSqlInstances = $env:COMPUTERNAME
)



[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

if (Get-PSRepository -Name "PSGallery") {
    Write-Verbose "PSGallery already registered"
} 
else {
    Write-Output "Registering PSGallery..."
    Register-PSRepository -Default
}

## you can add or remove additional modules here as needed
$modules = @("dbatools")
        
foreach ($module in $modules) {
    if (Get-Module -ListAvailable -Name $module) {
        Write-Output "$module already installed"
    } 
    else {
        Write-Output "Installing $module..."
        Install-Module $module -Force -SkipPublisherCheck -Scope CurrentUser | Out-Null
        Write-Output "Importing $module..."
        Import-Module $module -Force -PassThru -Scope Local | Out-Null
    }
}

Write-Output "Installing SqlWatch..."
Install-DbaSqlWatch -SqlInstance $TargetSqlInstances -Database SQLWATCH
