-- This is code originally written by Sebastian Meine
-- Source: https://sqlity.net/en/2460/sql-password-hash/

-- For SQL Server versions >= 2012 [SHA512]
DECLARE @pwd NVARCHAR(MAX) = 'plaintext-password';
DECLARE @salt VARBINARY(4) = CRYPT_GEN_RANDOM(4);
DECLARE @hash VARBINARY(MAX);

SET @hash = 0x0200 + @salt + HASHBYTES('SHA2_512', CAST(@pwd AS VARBINARY(MAX)) + @salt);
SELECT @hash AS NewHash, PWDCOMPARE(@pwd, @hash) AS IsPasswordHash;

GO

-- For SQL Server versions > 2000 and < 2012 [SHA1]
DECLARE @pswd NVARCHAR(MAX), @salt VARBINARY(4), @hash VARBINARY(MAX);
SET @pswd = 'plaintext-password';
SET @salt = CAST(NEWID() AS VARBINARY(4));

SET @hash = 0x0100 + @salt + HASHBYTES('SHA1', CAST(@pswd AS VARBINARY(MAX)) + @salt);
SELECT @hash AS HashValue, PWDCOMPARE(@pswd,@hash) AS IsPasswordHash;

GO

-- For SQL Server versions <= 2000 [case-insensitive SHA1]
DECLARE @pswd NVARCHAR(MAX), @salt VARBINARY(4), @hash VARBINARY(MAX);
SET @pswd = 'plaintext-password';
SET @salt = CAST(NEWID() AS VARBINARY(4));

SET @hash = 0x0100 + @salt + HASHBYTES('SHA1', CAST(@pswd AS VARBINARY(MAX)) + @salt) + HASHBYTES('SHA1', CAST(UPPER(@pswd) AS VARBINARY(MAX)) + @salt);
SELECT @hash AS HashValue, PWDCOMPARE(@pswd,@hash) AS IsPasswordHash;
