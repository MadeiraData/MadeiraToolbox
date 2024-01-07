$MyServer = "localhost"
$DBs = @(Get-SqlDatabase -Server $MyServer -Credential $sqlCredential | Where-Object {$_.Name -in @('Try002','Try001')})
foreach ($DB in $DBs) {
    Write-Host $DB.Name
    $dbName = $DB.Name
    $date = Get-Date -format "yyyyMMdd_hhmmss"
    Backup-SqlDatabase -ServerInstance localhost -Database $dbName -BackupFile C:\TEMP\Backup\$MyServer-$dbName-Full-$date.bak
}