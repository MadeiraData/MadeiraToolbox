#Set Variables:
$ServerName              = 'YOUR-SERVER-NAME'
$DatabaseName            = 'YOUR DB NAME'
$InputFile               = 'C:\temp\YourFolder\YourFile.sql' #  <===== replace with your .sql file location path (the .sql file containing your T-SQL code)
$TimeStamp               = Get-Date -Format "yyyyMMddHHmmss" #  <===== this will generate a unique timestamp which will be part of the new .csv file name
$Path                    = 'C:\temp\YourFolder\' #  <===== replace with your target folder path
$FileName                = 'PowershellTest'+'_'+$TimeStamp+'.csv' #  <==== in this case, the file name will look like "PowershellTest_20220702192532"
$FilePath                = $Path+$FileName #  <===== this will provide a value for the -Path parameter of the Export-Csv cmdlet
$MailSender              = 'you@yourmail.com' 
$MailRecipients          = 'recipient@recipientmail.com'
$MailSubject             = 'YOUR MAIL SUBJECT'
$MailBoy                 = 'YOUR MAIL BODY'
$MailServiceUserName     = 'you@yourmail.com' #  <===== your mail service user name
$MailServicePassword     = 'xxxxxxxxxxx' #  <===== your mail service password

#Get Data using Windows Authentication:
$Data                    = Invoke-Sqlcmd -ServerInstance $ServerName -Database $DatabaseName -InputFile $InputFile

#Get Data using SQL Authentication (in case you use SQL Server authentication, comment out the line above and uncomment the line below):
#$Data                   = Invoke-Sqlcmd -ServerInstance $ServerName -Database $DatabaseName -Username "YourUserName" -Password "YourPassword" -InputFile $InputFile

               
#Export the Data to a .csv file:                    
$Data | Export-Csv -Path $FilePath -NoTypeInformation


Send-MailMessage -From $MailSender `
                 -To $MailRecipients.Split(';') `
                 -Subject $MailSubject `
                 -Body $MailBoy `
                 -Attachment $FilePath `
                 -SmtpServer smtp.outlook.office365.com ` #  <===== your mail service SMTP Server
                 -Port 587 ` #  <===== your mail service port
                 -UseSsl `
                 -Credential (New-Object `
                 -TypeName System.Management.Automation.PSCredential `
                 -ArgumentList $MailServiceUserName, `
                  (ConvertTo-SecureString `
                 -String $MailServicePassword `
                 -AsPlainText -Force))