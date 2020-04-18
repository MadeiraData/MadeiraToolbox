$EmailFrom = " MadeiraBecomeADBA@gmail.com"
$EmailTo = "dudu@madeira.co.il " 
$Subject = "MainServer:SQL Agent Stopped " 
$Body = "Please look into the issue; Scheduled Jobs will not run if the SQL Server Agent Service remains stopped." 
$SMTPServer = "smtp.gmail.com" 
$SMTPClient = New-Object Net.Mail.SmtpClient($SmtpServer, 587) 
$SMTPClient.EnableSsl = $true 
$SMTPClient.Credentials = New-Object System.Net.NetworkCredential("MadeiraBecomeADBA@gmail.com", "301706867"); 
$SMTPClient.Send($EmailFrom, $EmailTo, $Subject, $Body)
