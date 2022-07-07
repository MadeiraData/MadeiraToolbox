CREATE FUNCTION [dbo].[IPv4ToNumeric]
(
	@IPv4 varchar(40)
)
RETURNS BIGINT
WITH SCHEMABINDING, RETURNS NULL ON NULL INPUT
AS
BEGIN
	DECLARE @RV BIGINT;
	
	DECLARE @Octets AS table
	(Loc int NOT NULL IDENTITY(1,1), Octet bigint NOT NULL)

	INSERT INTO @Octets (Octet)
	SELECT [value]
	FROM STRING_SPLIT(@IPv4, N'.')

	SELECT @RV = SUM(POWER(256, q.ReverseLoc - 1) * q.Octet)
	FROM
	(
	SELECT Octet, ReverseLoc = ROW_NUMBER() OVER (ORDER BY Loc DESC)
	FROM @Octets
	) AS q

	RETURN @RV;
END
GO
CREATE FUNCTION [dbo].[IPv6ToNumeric]
(
	@IPv6 varchar(40)
)
RETURNS BINARY(16)
WITH SCHEMABINDING, RETURNS NULL ON NULL INPUT
AS
BEGIN
	DECLARE @PaddedHex varchar(50);

	SELECT @PaddedHex = ISNULL(@PaddedHex,N'') + RIGHT('0000' + [value], 4)
	FROM STRING_SPLIT(@IPv6, ':')

	RETURN CONVERT(varbinary(MAX), @PaddedHex, 2)
END
GO
CREATE FUNCTION [dbo].[IPNumericToIPv4]
(
	@IPv4Numeric bigint
)
RETURNS varchar(40)
WITH SCHEMABINDING, RETURNS NULL ON NULL INPUT
AS
BEGIN
	DECLARE @RV varchar(40);

	SELECT @RV =  CAST((( @IPv4Numeric / POWER(256, n-1) ) % 256 ) AS varchar(40)) + ISNULL(N'.' + @RV, N'')
	FROM (VALUES (1),(2),(3),(4)) v(n)

	RETURN @RV;
END
GO
CREATE FUNCTION [dbo].[IPNumericToIPv6]
(
	@IPv6Numeric binary(16)
)
RETURNS varchar(40)
WITH SCHEMABINDING, RETURNS NULL ON NULL INPUT
AS
BEGIN
	DECLARE @RV varchar(40)

	SELECT
	 @RV = ISNULL(@RV + N':', N'') + SUBSTRING(PaddedOctet, PATINDEX('%[^0]%', PaddedOctet + '.'), LEN(PaddedOctet))
	FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8)) v(n)
	CROSS APPLY
	(VALUES(SUBSTRING(CONVERT(varchar(50), @IPv6Numeric, 2), (n-1)*4 + 1, 4))) AS s(PaddedOctet)

	RETURN LOWER(@RV)
END
GO