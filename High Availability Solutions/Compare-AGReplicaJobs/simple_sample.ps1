
Set-ExecutionPolicy Unrestricted -Scope CurrentUser

$ComputerName = "."
$outputFolder = ""
$emailFrom = "sql_alerts@acme.com"
$emailTo = @("dba@acme.com","it@acme.com")
$emailServerAddress = "smtp.acme.com"


Import-Module .\Compare-AGReplicaJobs.psd1;

Compare-AGReplicaJobs -ComputerName $ComputerName -outputFolder $outputFolder -Verbose #-From $emailFrom -To $emailTo -EmailServer $emailServerAddress

Remove-Module Compare-AGReplicaJobs