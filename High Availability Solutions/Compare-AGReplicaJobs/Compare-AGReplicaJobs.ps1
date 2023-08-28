#   This file is part of Compare-AGReplicaJobs.
#
#   Copyright 2020 Eitan Blumin <@EitanBlumin, https://www.eitanblumin.com>
#         while at Madeira Data Solutions <https://www.madeiradata.com>
#
#   Licensed under the MIT License (the "License");
# 
#   Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#   
#   The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#   
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


Function Compare-AGReplicaJobs
{
<#
.SYNOPSIS
Compare SQL Scheduled Jobs between Availability Group Replicas.
Compare-AGReplicaJobs Function: Compare-AGReplicaJobs
Author: Eitan Blumin (@EitanBlumin) | Madeira Data Solutions (@Madeira_Data)
License: MIT License
 
.DESCRIPTION
Compare-AGReplicaJobs Compares SQL Scheduled Jobs between Availability Group Replicas, based on the specified database context of each job step.
The cmdlet performs the following operations:
- Connects to the specified ComputerName and detects any configured Availability Groups.
- Detects the relevant replica databases and replica servers of each Availability Group.
- Connects to each AG replica and compares the relevant scheduled jobs between each secondary and primary replica.
- Generate a report in HTML summarizing the differences.
- Generate DROP/CREATE change-scripts to be applied to each secondary replica in order to "align" them to their primary counterpart.
- Send an e-mail message containing the HTML report, with the relevant change-scripts as attachments.

.EXAMPLE
C:\PS> # Minimum parameters:
C:\PS> Import-Module .\Compare-AGReplicaJobs.psd1; Compare-AGReplicaJobs -From "no-reply@acme-corp.com" -To "dba@acme-corp.com"

.EXAMPLE
C:\PS> # Specifying e-mail server, relevant SQL Server, and relevant job categories:
C:\PS> Import-Module .\Compare-AGReplicaJobs.psd1; Compare-AGReplicaJobs -From "no-reply@acme-corp.com" -To "dba@acme-corp.com" -EmailServer "smtp.acme-corp.com" -ComputerName "MySQLServer\MyInstanceName" -JobCategories "Production Jobs", "Acme Jobs"

.EXAMPLE
C:\PS> # Creating a PSCredential object for the -Credential parameter:
C:\PS> $username = "admin@domain.com"
C:\PS> $password = ConvertTo-SecureString "mypassword" -AsPlainText -Force
C:\PS> $psCred = New-Object System.Management.Automation.PSCredential -ArgumentList ($username, $password)
C:\PS> Import-Module .\Compare-AGReplicaJobs.psd1; Compare-AGReplicaJobs -From "db_alerts@domain.com" -To "sysadmin@domain.com" -EmailServer "smtp.domain.com" -Port 587 -UseSsl -Credential $psCred

.EXAMPLE
C:\PS> # Display help:
C:\PS> Import-Module .\Compare-AGReplicaJobs.psd1; Get-Help Compare-AGReplicaJobs -Full

.NOTES
Compare-AGReplicaJobs Compares SQL Scheduled Jobs between Availability Group Replicas, based on the specified database context of each job step.
This is an open-source project developed by Eitan Blumin while an employee at Madeira Data Solutions, Madeira Ltd.

To-do:
- Add a parameter that determines which server is the "master" job server (default empty - means use the PRIMARY replica)
- Add parameters to extend SQL Server connectivity options (Windows Authentication, SQL Authentication, Encrypt Connection)
- Add a multi-option parameter (or a set of switches) determining the output type(s):
    - Generate HTML report and script files
    - Send E-mail Report
    - Return HTML report as output
    - Return a Complex Object as output (containing the comparison result object, the HTML report, and the change-scripts)
- If Sending an E-Mail report is not specified, then make the e-mail related parameters optional.
- Add comment-based documentation for all parameters

.LINK
https://madeiradata.github.io/mssql-jobs-hadr

.LINK
https://madeiradata.com

.LINK
https://eitanblumin.com
#>
Param(
    
    [Parameter(Mandatory=$false, Position=0,
    HelpMessage="Enter the e-mail address of the sender.")]
    [Alias("From","Sender","EmailSender")]
    [ValidateNotNullOrEmpty()]
    [String]
    $emailFrom,
    
    [Parameter(Mandatory=$false, Position=1,
    HelpMessage="Enter a list of one or more e-mail addresses for the recipients.")]
    [Alias("To","Recipients","EmailRecipients")]
    [String[]]
    $emailTo = @(),
    
    [Parameter(Mandatory=$false, Position=2,
    HelpMessage='"Enter an address for the SMTP server to use for sending the e-mail. Default is $PSEmailServer.')]
    [Alias("EmailServer","SMTPServer","SMTP")]
    [ValidateNotNullOrEmpty()]
    [String]
    $emailServerAddress = $PSEmailServer,

    [Parameter(Mandatory=$false, Position=3,
    HelpMessage="Enter the SQL Server name to connect to and investigate.")]
    [Alias("CN","MachineName","SQLServer","ServerName")]
    [String]
    $ComputerName = ".",
    
    [Parameter(Mandatory=$false,
    HelpMessage="Enter a port number for the e-mail server. Default is 25.")]
    [Alias("Port","EmailPort","SMTPPort")]
    [Int32]
    $emailServerPort = 25,
    
    [Parameter(Mandatory=$false,
    HelpMessage="Enter a credential object for the e-mail server.")]
    [Alias("Credential","EmailCredentials")]
    [AllowNull()]
    [PSCredential]
    $emailCredential = $null,

    [Parameter(Mandatory=$false,
    HelpMessage="Enter a folder path where to save output report and change-scripts. Leave empty to use local temporary folder.")]
    [String]
    $outputFolder = "",
    
    [Parameter(Mandatory=$false,
    HelpMessage="Enter a list of one or more job categories to check. Leave empty to check all jobs.")]
    [AllowEmptyCollection()]
    [String[]]
    $JobCategories = @(),
    
    [Parameter(Mandatory=$false,
    HelpMessage="Specify whether to use SSL for the e-mail server.")]
    [Alias("UseSsl")]
    [Switch]
    $emailUseSSL
)
Begin
{
    $asm = [System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO')
    $ServerObject = New-Object Microsoft.SqlServer.Management.SMO.Server($ComputerName)
    
    $ScriptOptionsForDrop = New-Object Microsoft.SqlServer.Management.Smo.ScriptingOptions
    $ScriptOptionsForDrop.IncludeIfNotExists = $true
    $ScriptOptionsForDrop.ScriptDrops = $true
    $JobPropertiesToCompare = @("Name","Category""DeleteLevel","Description","EmailLevel","EventLogLevel","NetSendLevel","JobSteps","JobSchedules","OperatorToEmail","OperatorToNetSend","OperatorToPage","StartStepID","OwnerLoginName")

    function Get-CompareableJob{
    param([object]$JobObject)
        $JobSteps = @()
        $JobObject.JobSteps| ForEach-Object {
                            $currStep = @{
                                    Name=$_.Name;
                                    Command=$_.Command;
                                    CommandExecutionSuccessCode=$_.CommandExecutionSuccessCode;
                                    DatabaseName=$_.DatabaseName;
                                    DatabaseUserName=$_.DatabaseUserName;
                                    ID=$_.ID;
                                    JobStepFlags=$_.JobStepFlags;
                                    OnFailAction=$_.OnFailAction;
                                    OnFailStep=$_.OnFailStep;
                                    OnSuccessAction=$_.OnSuccessAction;
                                    OSRunPriority=$_.OSRunPriority;
                                    OutputFileName=$_.OutputFileName;
                                    ProxyName=$_.ProxyName;
                                    RetryAttempts=$_.RetryAttempts;
                                    RetryInterval=$_.RetryInterval;
                                    Server=$_.Server;
                                    SubSystem=$_.SubSystem
                                }
                            $JobSteps+=[pscustomobject]$currStep
                        }
        $JobSchedules = @()
        $JobObject.JobSchedules | ForEach-Object {
                            $currSchedule = @{
                                    Name=$_.Name;
                                    ActiveEndDate=$_.ActiveEndDate;
                                    ActiveEndTimeOfDay=$_.ActiveEndTimeOfDay;
                                    ActiveStartDate=$_.ActiveStartDate;
                                    ActiveStartTimeOfDay=$_.ActiveStartTimeOfDay;
                                    FrequencyInterval=$_.FrequencyInterval;
                                    FrequencyRecurrenceFactor=$_.FrequencyRecurrenceFactor;
                                    FrequencyRelativeIntervals=$_.FrequencyRelativeIntervals;
                                    FrequencySubDayInterval=$_.FrequencySubDayInterval;
                                    FrequencySubDayTypes=$_.FrequencySubDayTypes;
                                    IsEnabled=$_.IsEnabled
                                }
                            $JobSchedules+=[pscustomobject]$currSchedule
                        }
        [pscustomobject]@{
            Name=$JobObject.Name;
            Category=$JobObject.Category;
            DeleteLevel=$JobObject.DeleteLevel;
            Description=$JobObject.Description;
            EmailLevel=$JobObject.EmailLevel;
            EventLogLevel=$JobObject.EventLogLevel;
            NetSendLevel=$JobObject.NetSendLevel;
            JobSteps=$JobSteps;
            JobSchedules=$JobSchedules;
            OperatorToEmail=$JobObject.OperatorToEmail;
            OperatorToNetSend=$JobObject.OperatorToNetSend;
            OperatorToPage=$JobObject.OperatorToPage;
            StartStepID=$JobObject.StartStepID;
            OwnerLoginName=$JobObject.OwnerLoginName;
            JobID=$JobObject.JobID;
            DropScript=$JobObject.Script($ScriptOptionsForDrop).Replace("@job_id=N'$($JobObject.JobID)'","@job_name=N'$($JobObject.Name)'");
            CreateScript=$JobObject.Script()
         }
    }

}
Process
{
    $comparisonResults = @()

    # For each Availability Group
    foreach ($ag in $ServerObject.AvailabilityGroups | Where-Object { $_.State -eq "Existing" })
    {
        # Find which databases are involved in the AG
        $AGDatabases = @()
        $ag.AvailabilityDatabases | ForEach-Object { $AGDatabases += $_.Name }
        
        Write-Verbose "========= Availability Group: [$($ag.Name)] Primary Replica: [$($ag.PrimaryReplicaServerName)] ========="

        # Connect to PRIMARY replica to get the "master" list of jobs
        $AGPrimaryServer = $ag.AvailabilityReplicas | Where-Object { $_.Name -eq $ag.PrimaryReplicaServerName }
        $PrimarySMO = New-Object Microsoft.SqlServer.Management.SMO.Server($AGPrimaryServer.Name)
        $PrimaryReplicaJobs = @()

        # Get only relevant jobs based on their TSQL step database context
        $PrimarySMO.JobServer.Jobs | Where-Object { ($JobCategories -contains $_.Category -or $JobCategories.Count -eq 0) -and $_.JobType -eq "Local" } | ForEach-Object {
                            $currJob = Get-CompareableJob $_
                            $_.JobSteps | Where-Object {
                                                $AGDatabases -contains $_.DatabaseName -and $PrimaryReplicaJobs -notcontains $currJob 
                                            } | ForEach-Object {
                                                $PrimaryReplicaJobs += $currJob
                                            }
                        }
        Write-Verbose "=== Found: $($PrimaryReplicaJobs.Count) Job(s) for $($AGDatabases.Count) Database(s) in Primary Replica ==="
        
        # For each AG replica
        foreach ($replica in $ag.AvailabilityReplicas | Where-Object { $_.Name -ne $AGPrimaryServer.Name })
        {
            Write-Verbose "========================= Replica Server: [$($replica.Name)] ================"

            # Connect to SECONDARY replica to get its list of jobs
            $SecondarySMO = New-Object Microsoft.SqlServer.Management.SMO.Server($replica.Name)
            $SecondaryReplicaJobs = @()
        
            # Get only relevant jobs based on their TSQL step database context
            $SecondarySMO.JobServer.Jobs | Where-Object { ($JobCategories -contains $_.Category -or $JobCategories.Count -eq 0) -and $_.JobType -eq "Local" } | ForEach-Object {
                $currJob = Get-CompareableJob $_
                $_.JobSteps | Where-Object {
                                    $AGDatabases -contains $_.DatabaseName -and $SecondaryReplicaJobs -notcontains $currJob 
                                } | ForEach-Object {
                                    $SecondaryReplicaJobs += $currJob
                                }
            }

            # Create new comparison result
            $comparisonResults += [pscustomobject]@{
                    AGname=$ag.Name;
                    Primary=$ag.PrimaryReplicaServerName;
                    Secondary=$replica.Name;
                    Databases=$AGDatabases;
                    PrimaryReplicaJobs=$PrimaryReplicaJobs;
                    SecondaryReplicaJobs=$SecondaryReplicaJobs;
                    ComparedJobs=Compare-Object -ReferenceObject $PrimaryReplicaJobs -DifferenceObject $SecondaryReplicaJobs -Property $JobPropertiesToCompare
                    }
        }
    }
    
    $outputsList = @{}
    $htmlBody = ""

    # output summary results:
    ## uses the awesome Group-Object and ConvertTo-Html cmdlets
    ## and the disgusting IIF (immediate if) syntax of Powershell: (&{If($condition){$trueValue} else {$falseValue}})
    $comparisonResults | ForEach-Object {
        $currResultItem = $_
        $serverKey = $_.Secondary.Replace("\","_")
        $title = $_.AGname
        $subtitle= "Secondary: <b>" + $_.Secondary + "</b> | Primary: <b>" + $_.Primary + "</b>"
        $grp = $_.ComparedJobs | Group-Object Name

        $htmlBody += $grp `
            | Select @{N='Job';E={$_.Name}}, @{N='Target';E={$currResultItem.Secondary}}, @{N='Indicator';E={(&{If($_.Count -eq 1) {$_.Group[0].SideIndicator} Else {"<>"}})}}, @{N='Master';E={$currResultItem.Primary}} `
            | ConvertTo-Html -Property Job, Target, Indicator, Master -Fragment -PreContent "<h1>$title</h1><p>$subtitle</p>" -PostContent "<h3>Change Script</h3><ul><li><a href='cid:{changescript_$serverKey}'>{changescript_$serverKey}</a></li></ul>"
    }

    # Script Drop for jobs that are in the Difference object (Secondary Replica) and not in Reference object (Primary Replica)
    $comparisonResults | ForEach-Object {
        $currResultItem = $_
        $serverKey = $_.Secondary.Replace("\","_")
        
        if ($outputsList[$serverKey] -eq $null) {
            $outputsList[$serverKey] = ""
        }

        $currResultItem.ComparedJobs | Where-Object { $_.SideIndicator -ne "<=" } | ForEach-Object {
            $diffJobName = $_.Name
            $diffJob = $currResultItem.SecondaryReplicaJobs | Where-Object { $_.Name -eq $diffJobName }
            $outputsList[$serverKey] += $diffJob.DropScript
            $outputsList[$serverKey] += [Environment]::NewLine + "GO" + [Environment]::NewLine
        }
    }
    # Script Create for jobs that are in the Reference object (Primary Replica) and not in Difference object (Secondary Replica)
    $comparisonResults | ForEach-Object {
        $currResultItem = $_
        $serverKey = $_.Secondary.Replace("\","_")

        if ($outputsList[$serverKey] -eq $null) {
            $outputsList[$serverKey] = ""
        }

        $currResultItem.ComparedJobs | Where-Object { $_.SideIndicator -ne ">=" } | ForEach-Object {
            $diffJobName = $_.Name
            $diffJob = $currResultItem.SecondaryReplicaJobs | Where-Object { $_.Name -eq $diffJobName }
            $outputsList[$serverKey] += $diffJob.CreateScript
            $outputsList[$serverKey] += [Environment]::NewLine + "GO" + [Environment]::NewLine
        }
    }

    # Prepare array of change-scripts
    $filesToAttach = @()

    if ($outputFolder -eq "" -or -not (Test-Path $outputFolder)) {
        $outputFolder = [System.IO.Path]::GetTempPath()
    }

    foreach($name in $outputsList.keys)
    {
        if ($outputsList[$name] -ne $null -and $outputsList[$name] -ne "")
        {
            $sqlFileName = $name + "_align_jobs_$(Get-Date -Format "yyyyMMdd").sql"
            $sqlFilePath = $outputFolder + $sqlFileName
            $htmlFilePath = $outputFolder + "jobs_comparison_report_$(Get-Date -Format "yyyyMMdd").html"
        
            $filesToAttach += $sqlFilePath

            Write-Host "Output Script: $sqlFilePath"
            $outputsList[$name] | Out-File $sqlFilePath

            $htmlBody = $htmlBody.Replace("{changescript_$name}",$sqlFileName)
        }
    }
    
    # If discrepancies found, generate report
    if ($filesToAttach.Length -gt 0) {
        $reportBody = ConvertTo-Html -Body $htmlBody -PostContent "<p>Generated on $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")</p>"
        $reportBody | Out-File $htmlFilePath

        Write-Host "Discrepencies found. Report saved to: $($htmlFilePath)"
        
        # Send Email
        if ($emailTo.Length -gt 0 -and $emailFrom -ne "" -and $emailServerAddress -ne "") {

            Write-Verbose "Sending E-Mail..."
            
            if ($ComputerName -eq "." -or $ComputerName -eq "") {
                $ComputerName = (Get-ComputerInfo -Property CsName).CsName
            }
        
            # Use parameter splatting
            $mailParams = @{
                From = $emailFrom
                To = $emailTo
                Subject = "Job Discrepancies found - $ComputerName"
                Body = $($reportBody -join [Environment]::NewLine)
                BodyAsHtml = $true
                Attachments = $filesToAttach
                SmtpServer = $emailServerAddress
                Port = $emailServerPort
            }

            if ($emailUseSSL) {
                $mailParams["UseSsl"] = $true
            }

            if ($emailCredential -ne $null) {
                $mailParams["Credential"] = $emailCredential
            }

            Send-MailMessage @mailParams
        }

    } else {
        Write-Host "OK"
    }
}
}