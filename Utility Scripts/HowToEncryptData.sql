/*=======================================================================================
-----------------------------HOW TO ENCRYPT SENSISTIVE DATA------------------------------

Written By: Eric Rouach, Madeira Data Solutions
Date of Creation: March 2021

This series of scripts is based on the AdventureWorks2014 - [Sales].[CreditCard] Table

The process describes the encryption of the CardNumber column:

We will demonstrate two ways of encrypting data:

*encrypt and make data decryptable

**encrypt and make data undecryptable

=======================================================================================*/

--
USE
AdventureWorks2014;
GO

--Check  the [Sales].[CreditCard] table content:
SELECT * FROM [Sales].[CreditCard]

/*
Since it is unsafe to keep the CardNumber as clear-text, let's encrypt it:

We will demonstrate two ways of encrypting data:
*encrypt and make data decryptable
**encrypt and make data undecryptable

For both cases, we need to take the following actions first:

-Create and backup Master Key
-Create and backup Certificate
-Create a Symmetric Key
*/

--1) Create Master key:
CREATE MASTER KEY
ENCRYPTION BY PASSWORD = '$trongPa$$word'; --choose a strong password and keep it in a safe place!
GO

--2) Backup Master Key:
BACKUP MASTER KEY TO FILE = 'C:\EncryptionBackups\AW2014MasterKeyBackup'
ENCRYPTION BY PASSWORD = '$trongPa$$word';  
GO   

--3) Create certificate
CREATE CERTIFICATE AW2014Certificate
WITH SUBJECT = 'CreditCard_Encryption',
EXPIRY_DATE = '20991231';
GO

--4) Backup certificate
BACKUP CERTIFICATE AW2014Certificate TO FILE = 'C:\EncryptionBackups\AW2014CertificateBackup'   
GO  

--5) Create symmetric key
CREATE SYMMETRIC KEY AW2014SymKey
WITH ALGORITHM = AES_256
ENCRYPTION BY CERTIFICATE AW2014Certificate;
GO

--==================================
--*encrypt and make data decryptable
--==================================

--Add a varbinary datatype encrypted column
ALTER TABLE [Sales].[CreditCard]
ADD CardNumberEnc VARBINARY(250) NULL
GO

--Check the table
SELECT * FROM [Sales].[CreditCard]
GO

--Open the symmetric key
OPEN SYMMETRIC KEY AW2014SymKey
DECRYPTION BY CERTIFICATE AW2014Certificate;

	--Encrypt existing data
	UPDATE [Sales].[CreditCard]
	SET
	CardNumberEnc = 
	EncryptByKey(Key_GUID('AW2014SymKey'), CardNumber, 1, CONVERT(VARBINARY, CreditCardID))

	--Check the table
	SELECT * FROM [Sales].[CreditCard]

	--Make the new column Non-Nullable
	ALTER TABLE [Sales].[CreditCard]
	ALTER COLUMN CardNumberEnc VARBINARY(250) NOT NULL
	GO

	--Check the table
	SELECT * FROM [Sales].[CreditCard]

	--Drop old column
	DROP INDEX [AK_CreditCard_CardNumber] ON [Sales].[CreditCard]
	GO

	ALTER TABLE [Sales].[CreditCard]
	DROP COLUMN CardNumber

	EXEC sp_rename
	'Sales.CreditCard.CardNumberEnc', 'CardNumber', 'COLUMN';  
	GO

--Close the symmetric key
CLOSE SYMMETRIC KEY AW2014SymKey;

--Create a NonClustered index on the new column
CREATE NONCLUSTERED INDEX [AK_CreditCard_CardNumber] ON [Sales].[CreditCard]
(
[CardNumber]
)
GO

--Check the table
SELECT 
	CreditCardID, 
	CardType,
	CardNumber,
	ExpMonth,
	ExpYear, 
	ModifiedDate
FROM
	[Sales].[CreditCard]

--Select and decrypt the CardNumber
OPEN SYMMETRIC KEY AW2014SymKey
DECRYPTION BY CERTIFICATE AW2014Certificate;

	SELECT 
		CreditCardID, 
		CardType,
		CONVERT(NVARCHAR(25), DecryptByKey(CreditCard.[CardNumber], 1, CONVERT(varbinary, CreditCardID))) AS CardNumber,
		ExpMonth,
		ExpYear, 
		ModifiedDate
	FROM
		[Sales].[CreditCard]

CLOSE SYMMETRIC KEY AW2014SymKey;


--=========================================================

--=====================================
--**encrypt and make data undecryptable
--====================================

--Add a varbinary datatype encrypted column
ALTER TABLE [Sales].[CreditCard]
ADD CardNumberEnc VARBINARY(250) NULL
GO

--Check the table
SELECT * FROM [Sales].[CreditCard]

--Open the symmetric key
OPEN SYMMETRIC KEY AW2014SymKey
DECRYPTION BY CERTIFICATE AW2014Certificate;

	--Encrypt existing data
	UPDATE [Sales].[CreditCard]
	SET
	CardNumberEnc = 
	HASHBYTES('SHA2_256', CardNumber) 

	--Check the table
	SELECT * FROM [Sales].[CreditCard]

	--Make the new column Non-Nullable
	ALTER TABLE [Sales].[CreditCard]
	ALTER COLUMN CardNumberEnc VARBINARY(250) NOT NULL
	GO

	--Check the table
	SELECT * FROM [Sales].[CreditCard]

	--Drop old column
	DROP INDEX [AK_CreditCard_CardNumber] ON [Sales].[CreditCard]
	GO

	ALTER TABLE [Sales].[CreditCard]
	DROP COLUMN CardNumber

	EXEC sp_rename
	'Sales.CreditCard.CardNumberEnc', 'CardNumber', 'COLUMN';  
	GO

--Close the symmetric key
CLOSE SYMMETRIC KEY AW2014SymKey;

--Create a NonClustered index on the new column
CREATE NONCLUSTERED INDEX [AK_CreditCard_CardNumber] ON [Sales].[CreditCard]
(
[CardNumber]
)
GO

--Check the table
SELECT 
	CreditCardID, 
	CardType,
	CardNumber,
	ExpMonth,
	ExpYear, 
	ModifiedDate
FROM
	[Sales].[CreditCard]
