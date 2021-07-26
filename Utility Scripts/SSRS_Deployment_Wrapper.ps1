if ($cred -ne $null) {
    $response = Read-Host "Existing credentials were found stored for '$($cred.UserName)'. Do you want to reuse these credentials? ( Y | N )"
}

if ($cred -eq $null -or $response.ToLower() -notlike "y*") {
    $cred = Get-Credential -Message "Please enter username and password for the Report Server. Or, press ESC to logon without credentials."
    if ($cred -eq $null) {
        Write-Output "No credentials provided."
    }
}


.\SSRS_Deploy.ps1 -SourceFolder "C:\CustomerReports" `
-TargetReportServerUri "http://localhost/ReportServer" `
-Credential $cred `
-CustomAuthentication `
-TargetFolder "CustomerSSRS" `
-OverrideDataSourcePathForAll "/Data Sources/DS_Shared"