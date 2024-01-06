$BackupFolder="C:\temp\Backup\"
$NewDataPath="c:\Temp\Data\" #The Data files directory (for the new DB)
$NewLogPath="c:\Temp\Log\" #The Log file directory (for the new DB)
$DBs = Get-ChildItem $BackupFolder -Filter *.bak | Out-GridView -OutputMode Multiple
foreach ($File in $DBs) {
    Write-Host $File
    $dbname = $File.Name.replace('.bak','') #The new DB name
    $relocate = @() #This variable (for the Restore-SqlDatabase) is initialized as a list (and not an empty string), for the "With Move" part of the Restore command
    $OutputFile="C:\temp\$dbname.sql" #A script file will be created
    $BackupFile=$BackupFolder+$File.Name
    $dbfiles = Invoke-Sqlcmd -ServerInstance localhost -Database tempdb -Query "Restore FileListOnly From Disk='$BackupFile';" #The Restore FileListOnly output is inserted into a variable
    foreach($dbfile in $dbfiles){
      $DbFileName = $dbfile.PhysicalName | Split-Path -Leaf
      if($dbfile.Type -eq 'L'){
        $newfile = Join-Path -Path $NewLogPath -ChildPath $DbFileName #The Log part of the "With Move"
      } else {
        $newfile = Join-Path -Path $NewDataPath -ChildPath $DbFileName #The Data parts of the "With Move"
      }
      $relocate += New-Object Microsoft.SqlServer.Management.Smo.RelocateFile ($dbfile.LogicalName,$newfile)
    }
    Restore-SqlDatabase -ServerInstance localhost `
        -Database $dbname `
        -RelocateFile $relocate `
        -BackupFile "$BackupFile" `
        -RestoreAction Database #`
        #-Script | Out-File $OutputFile #An optional parameter: instead of executing the command, it will be exported into a script file.
}