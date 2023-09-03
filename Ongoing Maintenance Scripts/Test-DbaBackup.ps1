function Test-DbaBackup {
    <#
    .SYNOPSIS
        Quickly and easily tests the set of full backups based on folder path.
    .DESCRIPTION
        Restores all or some of the latest backups and performs a DBCC CHECKDB.
        1. Gathers information about the last full backups from a folder path
        2. Restores the backups to the Destination with a new name.
        3. The database is restored as "dbatools-testrestore-$databaseName" by default, but you can change dbatools-testrestore to whatever you would like using -Prefix
        4. The internal file names are also renamed to prevent conflicts with original database
        5. A DBCC CHECKDB is then performed
    .PARAMETER Path
        Path to SQL Server backup files.
        Paths passed in as strings will be scanned using the desired method, default is a non recursive folder scan
        Accepts multiple paths separated by ','
        Or it can consist of FileInfo objects, such as the output of Get-ChildItem or Get-Item. This allows you to work with
        your own file structures as needed
    .PARAMETER Destination
        The destination server to use to test the restore.
    .PARAMETER DestinationCredential
        Login to the target instance using alternative credentials. Accepts PowerShell credentials (Get-Credential).
        Windows Authentication, SQL Server Authentication, Active Directory - Password, and Active Directory - Integrated are all supported.
        For MFA support, please use Connect-DbaInstance.
    .PARAMETER Prefix
        A string which will be prefixed to the start of the restore Database's Name and its files.
        Default is "dbatools-testrestore-"
    .PARAMETER DestinationFolder
        Path to restore the SQL Server backups to on the target instance.
        All database files (data and log) will be restored to this location.
    .PARAMETER DirectoryRecurse
        If specified the specified directory will be recursed into.
    .PARAMETER NoCheck
        If this switch is enabled, DBCC CHECKDB will be skipped.
    .PARAMETER NoDrop
        If this switch is enabled, the newly-created test database will not be dropped.
    .NOTES
        Tags: DisasterRecovery, Backup, Restore
        Author: Eitan Blumin (@EitanBlumin), eitanblumin.com
        Website: https://madeiradata.com
        Copyright: (c) 2020 by Eitan Blumin, licensed under MIT
        License: MIT https://opensource.org/licenses/MIT
    .LINK
        https://github.com/sqlcollaborative/dbatools/issues/6594
    .LINK
        https://docs.dbatools.io/#Test-DbaLastBackup
    .LINK
        https://docs.dbatools.io/#Restore-DbaDatabase
    .EXAMPLE
        PS C:\> Test-DbaBackup -Path "\\sqlbackups\backups" -DirectoryRecurse -Destination localhost | ConvertTo-DbaDataTable | Write-DbaDataTable -SqlInstance localhost -Table dbatools.dbo.lastbackuptests -AutoCreateTable
        Determines the last full backup for ALL databases in the folder, attempts to restore all databases (with a different name and file structure), then performs a DBCC CHECKDB.
        Once the test is complete, the test restore will be dropped.
        The test results will be outputted to a database table and saved in an auto-created table dbatools.dbo.lastbackuptests
    .EXAMPLE
        PS C:\> Test-DbaBackup -Path "T:\Backup" -DirectoryRecurse -Destination localhost -DestinationFolder "C:\restoreChecks" | Out-GridView
        Determines the last full backup for ALL databases in the folder, attempts to restore all databases to a specific folder, then performs a DBCC CHECKDB.
        Once the test is complete, the test restore will be dropped.
        The test results will be displayed in a Powershell grid view for easy sorting and filtering.
    .EXAMPLE
        PS C:\> Test-DbaBackup -Path "C:\dbsmart\Temp" -DirectoryRecurse -Destination localhost -Verbose | Out-GridView
        This is what I used for my own internal debugging.
        Remove this example before submitting to dbatools pull request.
    .EXAMPLE
        PS C:\> $userName = 'MyUserName'
        PS C:\> $userPassword = 'MySuperSecurePassword'
        PS C:\> $secStringPassword = ConvertTo-SecureString $userPassword -AsPlainText -Force
        PS C:\> $credObject = New-Object System.Management.Automation.PSCredential ($userName, $secStringPassword)
        PS C:\> Test-DbaBackup -Path "C:\dbsmart\Temp" -DirectoryRecurse -Destination SQL-DBTESTS01 -DestinationCredential $credObject -Verbose | Out-GridView

        Similarly to the last example, but this time use a specific username and password by creating a PSCredential object.
    #>
    [CmdletBinding(SupportsShouldProcess)]
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute("PSAvoidUsingPlainTextForPassword", "", Justification = "For Parameters DestinationCredential and AzureCredential")]
    param (
        [string]$Path,
        [string]$Destination = ".",
        [object]$DestinationCredential,
        [string]$Prefix = "dbatools-testrestore-",
        [string]$DestinationFolder,
        [switch]$DirectoryRecurse,
        [switch]$NoCheck,
        [switch]$NoDrop
    )
    process {
        #region remove-this-before-submitting-to-dbatools

        function Start-DbccCheck {
            [CmdletBinding(SupportsShouldProcess)]
            param (
                [object]$server,
                [string]$dbname,
                [switch]$table
            )

            $servername = $server.name

            if ($Pscmdlet.ShouldProcess($sourceserver, "Running dbcc check on $dbname on $servername")) {
                if ($server.ConnectionContext.StatementTimeout = 0 -ne 0) {
                    $server.ConnectionContext.StatementTimeout = 0
                }

                try {
                    if ($table) {
                        $null = $server.databases[$dbname].CheckTables('None')
                        Write-Verbose "Dbcc CheckTables finished successfully for $dbname on $servername"
                    } else {
                        $null = $server.Query("DBCC CHECKDB ([$dbname]) WITH DATA_PURITY, EXTENDED_LOGICAL_CHECKS, TABLOCK, NO_INFOMSGS, ALL_ERRORMSGS")
                        Write-Verbose "Dbcc CHECKDB finished successfully for $dbname on $servername"
                    }
                    return "Success"
                } catch {
                    $message = $_.Exception
                    if ($null -ne $_.Exception.InnerException) { $message = $_.Exception.InnerException }

                    # english cleanup only sorry
                    try {
                        $newmessage = ($message -split "at Microsoft.SqlServer.Management.Common.ConnectionManager.ExecuteTSql")[0]
                        $newmessage = ($newmessage -split "Microsoft.SqlServer.Management.Common.ExecutionFailureException:")[1]
                        $newmessage = ($newmessage -replace "An exception occurred while executing a Transact-SQL statement or batch. ---> System.Data.SqlClient.SqlException:").Trim()
                        $message = $newmessage
                    } catch {
                        $null
                    }
                    return $message.Trim()
                }
            }
        }
        
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

        if (Get-PSRepository -Name "PSGallery") {
            Write-Verbose "$(Get-TimeStamp) PSGallery already registered"
        } 
        else {
            Write-Information "$(Get-TimeStamp) Registering PSGallery"
            Register-PSRepository -Default
        }

        $modules = @("PSFramework", "PSModuleDevelopment", "dbatools")
        
        foreach ($module in $modules) {
            if (Get-Module -ListAvailable -Name $module) {
                Write-Verbose "$module already installed"
            } 
            else {
                Write-Information "Installing $module"
                Install-Module $module -Force -SkipPublisherCheck -Scope CurrentUser -ErrorAction Stop -AllowClobber
                Import-Module $module -Force -Scope Local
            }
        }

        #endregion remove-this-before-submitting-to-dbatools
    
        try {
            $destserver = Connect-DbaInstance -SqlInstance $Destination -SqlCredential $DestinationCredential
        } catch {
            Stop-PSFunction -Message "Failed to connect to: $Destination." -Target $Destination -Continue
        }

        $restoreSplat = @{
            SqlInstance = $Destination;
            Path = $Path;
            DirectoryRecurse = $DirectoryRecurse;
            WithReplace = $true;
            GetBackupInformation = "backupinfo";
            RestoredDatabaseNamePrefix = $Prefix;
            DestinationFilePrefix = $Prefix;
            DestinationDataDirectory = $DestinationFolder;
            DestinationLogDirectory = $DestinationFolder
        }

        $restoreresults = Restore-DbaDatabase @restoreSplat

        Write-Verbose "Restored $($restoreresults.Count) Backup(s)"

        $restoreresults | ForEach-Object { 

            $backupheader = $dbccElapsed = $startRestore = $endRestore = $restoreElapsed = $startDbcc = $endDbcc = $dbsize = $null
            $success = $restoreresult = $dbccresult = "Skipped"

            $restoreObject = $_
            
            $ts = [timespan]::fromseconds($restoreObject.DatabaseRestoreTime.TotalSeconds)
            $restoreElapsed = "{0:HH:mm:ss}" -f ([datetime]$ts.Ticks)
        
            $backupfile = $restoreObject.BackupFile
            $backupheader = Read-DbaBackupHeader -SqlInstance $destserver -Path $backupfile

            $dbname = $restoreObject.Database
            $destdb = $destserver.databases[$dbname]
            $source = $restoreObject.SqlInstance
            
            Write-Verbose "Processing $dbname restored from $backupfile"

            $backupinfo | Where-Object {$_.Database -eq $dbname} | ForEach-Object {
                $startRestore = $_.Start
                $endRestore = $_.End
                $dbsize = $_.TotalSize
            }
            
            if ($restoreObject.RestoreComplete -eq $true) {
                $restoreresult = $success = "Success"
            } else {
                if ($errormsg) {
                    $restoreresult = $success = $errormsg
                } else {
                    $success = "Failure"
                }
            }

            if (-not $NoCheck) {
                # shouldprocess is taken care of in Start-DbccCheck
                if ($dbname -eq ($Prefix + "master")) {
                    $dbccresult = "Skipped (not supported in restored master)"
                }
                elseif ($success -eq "Success" -and $null -eq $destserver.databases[$dbname])
                {
                    $dbccresult = "Skipped (DB Removed)"
                }
                elseif ($success -eq "Success")
                {
                    Write-Verbose "Starting DBCC."

                    $startDbcc = Get-Date
                    $dbccresult = Start-DbccCheck -Server $destserver -DbName $dbname 3>$null
                    $endDbcc = Get-Date

                    $dbccts = New-TimeSpan -Start $startDbcc -End $endDbcc
                    $ts = [timespan]::fromseconds($dbccts.TotalSeconds)
                    $dbccElapsed = "{0:HH:mm:ss}" -f ([datetime]$ts.Ticks)
                }
                else
                {
                    $dbccresult = "Skipped (Restore Failed)"
                }
            }

            if (-not $NoDrop -and $null -ne $destserver.databases[$dbname]) {
                if ($Pscmdlet.ShouldProcess($dbname, "Dropping Database $dbname on $destination")) {
                    Write-Verbose "Dropping database."

                    ## Drop the database
                    try {
                        #Variable $removeresult marked as unused by PSScriptAnalyzer replace with $null to catch output
                        $null = Remove-DbaDatabase -SqlInstance $destserver -Database $dbname -Confirm:$false
                        Write-Verbose "Dropped $dbname Database on $destination."
                    } catch {
                        $destserver.Databases.Refresh()
                        if ($destserver.databases[$dbname]) {
                            Write-Warning "Failed to Drop database '$dbname' on server '$destination'."
                        }
                    }
                }
            }

            $destserver.Databases.Refresh()
            if ($destserver.Databases[$dbname] -and -not $NoDrop) {
                Write-Warning "$dbname was not dropped."
            }
    

            if ($Pscmdlet.ShouldProcess("console", "Showing results")) {
                [pscustomobject]@{
                    SourceServer   = $backupheader.ServerName
                    TestServer     = $destination
                    Database       = $backupheader.DatabaseName
                    FileExists     = $true
                    Size           = [dbasize]($dbsize)
                    RestoreResult  = $success
                    DbccResult     = $dbccresult
                    RestoreStart   = [dbadatetime]$startRestore
                    RestoreEnd     = [dbadatetime]$endRestore
                    RestoreElapsed = $restoreElapsed
                    DbccStart      = [dbadatetime]$startDbcc
                    DbccEnd        = [dbadatetime]$endDbcc
                    DbccElapsed    = $dbccElapsed
                    BackupDates    = [String[]]($backupheader.BackupStartDate)
                    BackupFiles    = $backupfile
                }
            }
        }
    }
}
