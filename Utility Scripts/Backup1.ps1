$MyServer = "localhost"
$MyFolder = "C:\TEMP\Backup"
$DBs = @("Try001","Try002")
foreach ($DB in $DBs) {
    Write-Host $DB
    $date = Get-Date -format "yyyyMMdd_hhmmss"
    Backup-SqlDatabase -ServerInstance $MyServer -Database $DB -BackupFile $MyFolder\$MyServer-$DB-Full-$date.bak
}