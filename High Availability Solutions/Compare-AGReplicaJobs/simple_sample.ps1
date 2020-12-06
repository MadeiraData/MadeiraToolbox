
$ComputerName = "."
$outputFolder = ""
$emailFrom = "sql_alerts@acme.com"
$emailTo = @("dba@acme.com","it@acme.com")
$emailServerAddress = "smtp.acme.com"


Import-Module .\Compare-AGReplicaJobs.psd1;

Compare-AGReplicaJobs -From $emailFrom -To $emailTo -EmailServer $emailServerAddress -ComputerName $ComputerName -outputFolder $outputFolder -Verbose

Remove-Module Compare-AGReplicaJobs