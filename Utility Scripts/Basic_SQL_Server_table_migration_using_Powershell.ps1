# Author: Nathan Lifshes
# Date: 2020-01-13
Param (
      [string] $SrcServer ,
      [string] $SrcDatabase ,
      [string] $DestServer ,     
      [string] $DestDatabase ,
      [string] $SrcUsername = "" ,
      [string] $SrcPassword = "" ,
      [string] $DestUsername = "" ,
      [string] $DestPassword = "" ,
      [switch] $DoNotDeleteDestinationTable = $false
) 

Function ConnectionString([string] $ServerName, [string] $DbName, [string] $Username = "", [string] $Password = "") 
{
    $str = "Data Source=$ServerName;Initial Catalog=$DbName;"
    
    if ($Username -eq "") {
        $str = $str + "Integrated Security=True;"
    } else {
        $str = $str + "User ID=$Username;Password=$Password;"
    }

    $str
} 

If ( ! (Get-module SqlServer )) {
    Write-Output "Import-Module SqlServer"
    Import-Module "C:\Program Files\WindowsPowerShell\Modules\SqlServer\SqlServer.psd1" -Force
} 

$mySrvConn = new-object Microsoft.SqlServer.Management.Common.ServerConnection
$mySrvConn.ServerInstance=$SrcServer
$srv = new-object Microsoft.SqlServer.Management.SMO.Server($mySrvConn)
$db = $srv.Databases[$SrcDatabase]
 
# this takes all tables except some filters for my own database
$tbl = $db.tables | Where-object { -not $_.IsSystemObject -and $_.Name -notlike "BATCH*" -and $_.Name -notlike "*raw*" -and $_.Name -notlike "bak*" -and $_.Name -notlike "DATABASECHANGELOG*"}
 
$SrcConnStr = ConnectionString $SrcServer $SrcDatabase $SrcUsername $SrcPassword
$DestConnStr = ConnectionString $DestServer $DestDatabase $DestUsername $DestPassword
 
Try{
    Invoke-Sqlcmd -ServerInstance $DestServer -Database $DestDatabase -Query "EXEC sp_MSForEachTable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'"

    if ($DoNotDeleteDestinationTable -eq $false) {
        Invoke-Sqlcmd -ServerInstance $DestServer -Database $DestDatabase -Query "EXEC sp_MSForEachTable 'DELETE FROM ? '"
    }
}
Catch{
    $ex = $_.Exception
    Write-Host $ex.Message
}
 
foreach ($obj in $tbl) {
    Write-Host "Migrating Data Table $obj " -ForegroundColor Yellow
   
    Try
    {
        $SrcConn  = New-Object System.Data.SqlClient.SQLConnection($SrcConnStr)
        $CmdText = "SELECT * FROM " + $obj
        $SqlCommand = New-Object system.Data.SqlClient.SqlCommand($CmdText, $SrcConn)  
        $SrcConn.Open()
        [System.Data.SqlClient.SqlDataReader] $SqlReader = $SqlCommand.ExecuteReader()
 
        $bulkCopy = New-Object Data.SqlClient.SqlBulkCopy($DestConnStr, [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity)
        $bulkCopy.BatchSize = 5000
        $bulkCopy.BulkCopyTimeout = 90
        $bulkCopy.DestinationTableName = $obj
        $bulkCopy.WriteToServer($sqlReader)
    
        Write-Host "Finished Importing $obj"  -ForegroundColor Green
    }
    Catch [System.Exception]
    {
        $ex = $_.Exception
        Write-Host $ex.Message
    } 
} 

Try{
    Invoke-Sqlcmd -ServerInstance $DestServer -Database $DestDatabase -Query "EXEC sp_MSForEachTable 'ALTER TABLE ? CHECK CONSTRAINT ALL'"
}
Catch{
    $ex = $_.Exception
    Write-Host $ex.Message
}
 
$srv.ConnectionContext.Disconnect()