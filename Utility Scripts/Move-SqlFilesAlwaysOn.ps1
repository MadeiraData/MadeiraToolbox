#   Copyright 2021 Eitan Blumin <@EitanBlumin, https://www.eitanblumin.com>
#         while at Madeira Data Solutions <https://www.madeiradata.com>
#
#   Licensed under the MIT License (the "License");
# 
#   Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#   
#   The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#   
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

<#
.SYNOPSIS
Moves files to a new location for a SQL Server database in an AlwaysOn Availability Group.

Author: Eitan Blumin (@EitanBlumin) | Madeira Data Solutions (@Madeira_Data)
License: MIT License
 
.DESCRIPTION
Moves files to a new location for a SQL Server database in an AlwaysOn Availability Group.
The script performs the following operations:
- Makes sure the destination folder(s) exist.
- Suspends and removes the specified database from its Availability Group.
- Executes ALTER DATABASE .. MODIFY FILE .. to change the database file paths.
- If connected to the PRIMARY replica: Takes the database offline.
- If connected to the SECONDARY replica: Takes the whole MSSQLSERVER service down.
- Actually moves the files to their new destination, while retaining the file permissions and ownership (so that the SQL Server won't get an "Access Denied" error).
- Brings the database / service back online.
- Adds the database back to the Availability Group.

.EXAMPLE
C:\PS> .\Move-SqlFilesAlwaysOn.ps1 -DatabaseName "TestDB" -NewDataFolderPath "D:\MSSQL\Data\" -NewLogFolderPath "L:\MSSQL\Log\"

.NOTES
This is an open-source project developed by Eitan Blumin while an employee at Madeira Data Solutions, Madeira Ltd.

A few remarks:
- Note the parameters for the script. Be sure you're providing the proper values as input.
- The script was not tested with database files that aren't mdf/ndf/ldf (such as FILESTREAM, Full-Text Catalogs, and In-Memory). So, I cannot guarantee that those will work. But if you're feeling adventurous, you can use the switch parameter "AllowNonDataOrLogFileTypes" to try it out anyway.
- If you're moving large files, remember that it would take a while to finish, during which time the database will not be available.
- Don't forget to disable backup jobs! The script doesn't do that for you.
- The script is NOT idempotent. If it fails in the middle of execution, you may have trouble running it again.
- As always with such things: Test, test, test! I recommend adding a new, small database to your Availability Group and test the script on this database, before trying it out on your larger production databases.

.LINK
https://github.com/MadeiraData/MadeiraToolbox/blob/master/Utility%20Scripts/Move-SqlFilesAlwaysOn.ps1

.LINK
https://madeiradata.com

.LINK
https://eitanblumin.com

.LINK
https://www.sqlshack.com/a-walk-through-of-moving-database-file-in-sql-server-always-on-availability-group/
#>
param
(
[Parameter(Mandatory=$true, Position=0)][string]$DatabaseName = "TestDB",
[Parameter(Mandatory=$true, Position=1)][string]$NewDataFolderPath = "D:\MSSQL\Data\",
[Parameter(Mandatory=$false, Position=2)][string]$NewLogFolderPath = $NewDataFolderPath,
[string]$SqlInstanceConnectionString = "Data Source=.;Integrated Security=True;Application Name=Move Availability Group Files",
[string]$SqlServiceName = "MSSQLSERVER",
[switch]$SecondaryOnly,
[switch]$AllowNonDataOrLogFileTypes,
[switch]$SkipRunningJobsCheck,
[int32]$MaxRecoveryQueueMB = 1024,
[string]$logFileFolderPath = "C:\Madeira\log",
[string]$logFilePrefix = "move_ag_dbfiles_",
[string]$logFileDateFormat = "yyyyMMdd_HHmmss",
[int]$logFileRetentionDays = 7
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

$ErrorActionPreference = "Stop"

#region main

# Get metadata for the given database, and do some basic validation
$AGName = $null
$IsSecondary = $false

$result = Invoke-Sqlcmd -ConnectionString $SqlInstanceConnectionString -Query "SELECT ag.name,
        replica_states.role_desc,
		CONVERT(sysname, SERVERPROPERTY('ServerName')) AS ServerName
FROM sys.databases db
INNER JOIN sys.dm_hadr_availability_replica_states replica_states ON db.replica_id = replica_states.replica_id
INNER JOIN sys.availability_groups ag ON replica_states.group_id = ag.group_id
WHERE db.name = '$DatabaseName'" -Verbose -AbortOnError -OutputSqlErrors $true -ErrorAction Stop

if ($result)
{
    $AGName = $result['name']

    if ($result['ServerName'] -ne $env:COMPUTERNAME)
    {
        Write-Error "This script must be run locally from within one of the AG replica servers." -ErrorAction Stop
    }

    if ($result['role_desc'] -eq 'SECONDARY') {
        $IsSecondary = $true
    } elseif ($SecondaryOnly) {
        Write-Error "This script must be run locally from within the SECONDARY replica only." -ErrorAction Stop
    }
} else {
    Write-Error "Database [$DatabaseName] is not part of an availability group." -ErrorAction Stop
}

# Make sure this script is run as an Administrator
If (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole( [Security.Principal.WindowsBuiltInRole] “Administrator”) )
{
    Write-Error “You do not have Administrator rights to run this script!`nPlease re-run this script as an Administrator!” -ErrorAction Stop
}

if ($IsSecondary) {
    # Get the MSSQLSERVER service object
    try {
        $SqlService = Get-Service -Name $SqlServiceName

        if ($SqlService.Status -eq "Running" -and !$SqlService.CanStop) {
            Write-Error "No access to shut down '$SqlServiceName' service of computer '$env:COMPUTERNAME'" -ErrorAction Stop
        }
    } catch {
        Write-Error "Error accessing '$SqlServiceName' service of computer '$env:COMPUTERNAME'" -ErrorAction Stop
    }

    # Get all services dependent on the MSSQLSERVER service
    $SqlDependentServices = Get-Service -Name $SqlServiceName -DependentServices -ErrorAction Stop

    # Check for high recovery queues
    $recoveryQueues = Invoke-Sqlcmd -ConnectionString $SqlInstanceConnectionString -Query "select counter_name, instance_name, cntr_value
from sys.dm_os_performance_counters
where object_name like '%Database Replica%'
and counter_name like 'Recovery Queue%'
and cntr_value > $MaxRecoveryQueueMB * 1024
order by cntr_value desc" -Verbose -AbortOnError -ErrorAction Stop

    if ($recoveryQueues) {
        $recoveryQueues | Out-Host
        Write-Error "Stopping execution because Availability Group Recovery Queue is too high. Stopping the SQL service at this state may put the Availability Group at risk." -ErrorAction Stop
    }
}

while (!$SkipRunningJobsCheck)
{
    $runningJobs = Invoke-Sqlcmd -ConnectionString $SqlInstanceConnectionString -Query "SELECT j.name, ja.start_execution_date, ja.stop_execution_date, ja.next_scheduled_run_date
    FROM msdb..sysjobactivity as ja
    inner join msdb..sysjobs as j on ja.job_id = j.job_id
    WHERE session_id = (SELECT MAX(session_id) FROM msdb..sysjobactivity)
    AND start_execution_date IS NOT NULL
    AND (stop_execution_date is null or next_scheduled_Run_date between GETDATE() and DATEADD(minute, 5, GETDATE()))" -Verbose -AbortOnError -ErrorAction Stop

    if ($runningJobs)
    {
        $runningJobs | Out-Host
        $inputResponse = Read-Host "There are jobs currently running or soon to be executed. Are you sure you want to proceed anyway? (C=Continue, R=Retry Check, S=Stop) "
        if ($inputResponse.ToLower().StartsWith("c")) {
            $SkipRunningJobsCheck = $true;
        }
        elseif ($inputResponse.ToLower().StartsWith("s")) {
            Write-Error "Stopping execution because found currently running or soon to be run jobs." -ErrorAction Stop
        }
    }
}

# Use T-SQL to make sure target folder(s) exist with proper permissions
@($NewDataFolderPath,$NewLogFolderPath) | ForEach {
Invoke-Sqlcmd -ConnectionString $SqlInstanceConnectionString -Query "set nocount on;
declare @path varchar(8000) = '$_';

create table #tmp (
[FILE_EXISTS]			int	not null,
[FILE_IS_DIRECTORY]		int	not null,
[PARENT_DIRECTORY_EXISTS]	int	not null)

insert into #tmp
exec xp_fileexist @path;

if exists ( select * from #tmp where FILE_EXISTS = 1 )
begin
	raiserror(N'Existing path is not a directory: %s',16,1,@path);
end
else if exists (select * from #tmp where FILE_EXISTS = 0 AND FILE_IS_DIRECTORY = 0)
begin
	raiserror(N'Creating new directory: %s', 0, 1, @path);
	exec xp_create_subdir @path
end" -Verbose -AbortOnError -OutputSqlErrors $true -ErrorAction Stop
}

# Make sure the new folder paths are accessible
if (!$NewDataFolderPath -or !(Test-Path $NewDataFolderPath -PathType Container)) {
    Write-Error "Unable to access new data folder path: $NewDataFolderPath" -ErrorAction Stop
}
if (!$NewLogFolderPath -or !(Test-Path $NewLogFolderPath -PathType Container)) {
    Write-Error "Unable to access new log folder path: $NewLogFolderPath" -ErrorAction Stop
}

# Get metadata for all database files
$dbFiles = Invoke-Sqlcmd -ConnectionString $SqlInstanceConnectionString -Query "SELECT name, type_desc, physical_name
FROM sys.master_files
WHERE database_id = DB_ID('$DatabaseName');" -Verbose -AbortOnError -OutputSqlErrors $true -ErrorAction Stop

# Validate the path for each file and make sure it's either LOG or ROWS
# (I cannot guarantee at this point that this script will work with other file types such as FILESTREAM, Full-Text, In-Memory, etc.)
$dbFiles | ForEach {
    if ((Test-Path $_.physical_name -PathType Leaf -IsValid -ErrorAction Continue) -and ($_.type_desc -eq "ROWS" -or $_.type_desc -eq "LOG" -or $AllowNonDataOrLogFileTypes))
    {
        Write-Host "OK: $($_.name), type: $($_.type_desc), old path: $($_.physical_name)" -ForegroundColor Green
    } elseif ($_.type_desc -ne "ROWS" -and $_.type_desc -ne "LOG") {
        Write-Error "UNSUPPORTED TYPE: $($_.name), type: $($_.type_desc), old path: $($_.physical_name)" -ErrorAction Stop
    } else {
        Write-Error "INACCESSIBLE: $($_.name), type: $($_.type_desc), old path: $($_.physical_name)" -ErrorAction Stop
    }
}

# Suspend and remove the database from the Availability Group
Write-Output "$(Get-TimeStamp) Removing [$DatabaseName] from Availability Group [$AGName]"

$cmd = "USE [master];
ALTER DATABASE [$DatabaseName] SET HADR SUSPEND;"

if ($IsSecondary) {
    $cmd = $cmd + "
ALTER DATABASE [$DatabaseName] SET HADR OFF;"
} else {
    $cmd = $cmd + "
ALTER AVAILABILITY GROUP [$AGName] REMOVE DATABASE [$DatabaseName];"
}

Invoke-Sqlcmd -ConnectionString $SqlInstanceConnectionString -Query $cmd -Verbose -AbortOnError -OutputSqlErrors $true -ErrorAction Stop | Out-Null

# Update the database file paths with their new locations
$dbFiles | ForEach {

    $currFileName = Split-Path $_.physical_name -Leaf

    if ($_.type_desc -eq "ROWS") {
        $currNewPath = Join-Path -Path $NewDataFolderPath -ChildPath $currFileName
    } else {
        $currNewPath = Join-Path -Path $NewLogFolderPath -ChildPath $currFileName
    }
    
    Write-Output "$(Get-TimeStamp) Altering file [$($_.name)] with new path: $currNewPath"

    Invoke-Sqlcmd -ConnectionString $SqlInstanceConnectionString -Query "USE [master];
    ALTER DATABASE [$DatabaseName] MODIFY FILE (NAME = [$($_.name)], FILENAME = '$currNewPath');" -Verbose -AbortOnError -OutputSqlErrors $true -ErrorAction Stop | Out-Null
}

if (!$IsSecondary) {

    # If connected to the PRIMARY replica, take the database offline
    Write-Output "$(Get-TimeStamp) Taking [$DatabaseName] offline"

    Invoke-Sqlcmd -ConnectionString $SqlInstanceConnectionString -Query "USE [master];
    ALTER DATABASE [$DatabaseName] SET OFFLINE WITH ROLLBACK IMMEDIATE;" -Verbose -AbortOnError -OutputSqlErrors $true -ErrorAction Stop | Out-Null

} elseif ($SqlService.Status -eq "Running") {

    # If connected to the SECONDARY replica, take the MSSQLSERVER service offline
    Write-Output "$(Get-TimeStamp) Stopping service '$SqlServiceName' in computer '$env:COMPUTERNAME'..."

    $SqlService | Stop-Service -Force -Verbose

} else {
    Write-Warning "$(Get-TimeStamp) Service status of '$SqlServiceName' in computer '$env:COMPUTERNAME' is: $($SqlService.Status)"
}

# Begin moving the files to their new locations
$dbFiles | ForEach {

    $currFileName = Split-Path $_.physical_name -Leaf

    # Save the current file's permissions and ownership details
    $currAcl = Get-Acl -Path $_.physical_name

    if ($_.type_desc -eq "LOG") {
        $currNewPath = Join-Path -Path $NewLogFolderPath -ChildPath $currFileName
    } else {
        $currNewPath = Join-Path -Path $NewDataFolderPath -ChildPath $currFileName
    }
    
    Write-Output "$(Get-TimeStamp) Moving file [$($_.name)] from: $($_.physical_name) to: $currNewPath"

    Move-Item -Path $_.physical_name -Destination $currNewPath -Verbose -ErrorAction Stop | Out-Null

    # Apply the file-level permissions and ownership in their new location
    $currAcl | Set-Acl -Path $currNewPath -Verbose
}


if (!$IsSecondary) {

    # If connected to the PRIMARY replica, bring the database back online
    Write-Output "$(Get-TimeStamp) Bringing [$DatabaseName] online"

    Invoke-Sqlcmd -ConnectionString $SqlInstanceConnectionString -Query "USE [master];
    ALTER DATABASE [$DatabaseName] SET ONLINE;" -Verbose -AbortOnError -OutputSqlErrors $true -ErrorAction Stop | Out-Null

} elseif ($SqlService.Status -eq "Stopped") {

    # If connected to the SECONDARY replica, bring the MSSQLSERVER service and its dependent services back online
    Write-Output "$(Get-TimeStamp) Starting service '$SqlServiceName' in computer '$env:COMPUTERNAME'..."

    $SqlService | Start-Service -Verbose
    $SqlDependentServices | Start-Service -Verbose

} else {
    Write-Warning "$(Get-TimeStamp) Service status of '$SqlServiceName' in computer '$env:COMPUTERNAME' is: $($SqlService.Status)"
}

# Add the database back to the Availability Group
Write-Output "$(Get-TimeStamp) Joining database [$DatabaseName] back to Availability Group [$AGName]"

if ($IsSecondary) {
    $cmd = "ALTER DATABASE [$DatabaseName] SET HADR AVAILABILITY GROUP = [$AGName];"
} else {
    $cmd = "ALTER AVAILABILITY GROUP [$AGName] ADD DATABASE [$DatabaseName];"
}

Invoke-Sqlcmd -ConnectionString $SqlInstanceConnectionString -Query $cmd -Verbose -AbortOnError -OutputSqlErrors $true -ErrorAction Stop

# Output new file details
Invoke-Sqlcmd -ConnectionString $SqlInstanceConnectionString -Query "SELECT name, type_desc, physical_name, state_desc
FROM sys.master_files
WHERE database_id = DB_ID('$DatabaseName');" -Verbose -AbortOnError -OutputSqlErrors $true -ErrorAction Stop

Write-Output "$(Get-TimeStamp) Done"

#endregion main


#region finalization
if ($logFileFolderPath -ne "") { Stop-Transcript }
#endregion finalization
}