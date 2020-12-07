Param
(
[string]$logFileFolderPath = "C:\Madeira\log",
[string]$logFilePrefix = "my_ps_script_",
[string]$logFileDateFormat = "yyyyMMdd_HHmmss",
[int]$logFileRetentionDays = 30
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


Write-Output "$(Get-TimeStamp) Replace this code with your actual script body"


#region finalization
if ($logFileFolderPath -ne "") { Stop-Transcript }
#endregion finalization
}