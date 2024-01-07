$MyServer = "localhost"
$MyFolder = "C:\TEMP\Backup"
$DBs = Get-SqlDatabase -ServerInstance $MyServer | Out-GridView -OutputMode Multiple
foreach ($DB in $DBs) {
    $dbName = $DB.Name
    Write-Host $dbName
    $date = Get-Date -format "yyyyMMdd_hhmmss"
    Backup-SqlDatabase -ServerInstance $MyServer -Database $dbName -BackupFile $MyFolder\$MyServer-$dbName-Full-$date.bak
    }