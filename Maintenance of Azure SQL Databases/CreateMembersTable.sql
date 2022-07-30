CREATE TABLE
	dbo.Members
(
	Id						INT				NOT NULL	IDENTITY (1,1) ,
	Username				NVARCHAR(10)	NOT NULL ,
	Password				NVARCHAR(10)	NOT NULL ,
	FirstName				NVARCHAR(20)	NOT NULL ,
	LastName				NVARCHAR(20)	NOT NULL ,
	StreetAddress			NVARCHAR(100)	NULL ,
	CountryId				TINYINT			NOT NULL ,
	PhoneNumber				NVARCHAR(20)	NULL ,
	EmailAddress			NVARCHAR(100)	NOT NULL ,
	GenderId				TINYINT			NOT NULL ,
	BirthDate				DATE			NOT NULL ,
	SexualPreferenceId		TINYINT			NULL ,
	MaritalStatusId			TINYINT			NULL ,
	Picture					VARBINARY(MAX)	NULL ,
	RegistrationDateTime	DATETIME2(0)	NOT NULL
)

DECLARE
	@tblFirstNames
TABLE
(
	Name		NVARCHAR(20)	NOT NULL ,
	GenderId	TINYINT			NOT NULL
);

INSERT INTO
	@tblFirstNames
(
	Name ,
	GenderId
)
SELECT
	Name		= N'John' ,
	GenderId	= 1

UNION ALL

SELECT
	Name		= N'David' ,
	GenderId	= 1

UNION ALL

SELECT
	Name		= N'James' ,
	GenderId	= 1

UNION ALL

SELECT
	Name		= N'Ron' ,
	GenderId	= 1

UNION ALL

SELECT
	Name		= N'Bruce' ,
	GenderId	= 1

UNION ALL

SELECT
	Name		= N'Bryan' ,
	GenderId	= 1

UNION ALL

SELECT
	Name		= N'Gimmy' ,
	GenderId	= 1

UNION ALL

SELECT
	Name		= N'Rick' ,
	GenderId	= 1

UNION ALL

SELECT
	Name		= N'Paul' ,
	GenderId	= 1

UNION ALL

SELECT
	Name		= N'Phil' ,
	GenderId	= 1

UNION ALL

SELECT
	Name		= N'Laura' ,
	GenderId	= 2

UNION ALL

SELECT
	Name		= N'Jane' ,
	GenderId	= 2

UNION ALL

SELECT
	Name		= N'Sara' ,
	GenderId	= 2

UNION ALL

SELECT
	Name		= N'Lian' ,
	GenderId	= 2

UNION ALL

SELECT
	Name		= N'Rita' ,
	GenderId	= 2

UNION ALL

SELECT
	Name		= N'Samantha' ,
	GenderId	= 2

UNION ALL

SELECT
	Name		= N'Suzan' ,
	GenderId	= 2

UNION ALL

SELECT
	Name		= N'Marry' ,
	GenderId	= 2

UNION ALL

SELECT
	Name		= N'Monica' ,
	GenderId	= 2

UNION ALL

SELECT
	Name		= N'Julia' ,
	GenderId	= 2

UNION ALL

SELECT
	Name		= N'Shila' ,
	GenderId	= 2

UNION ALL

SELECT
	Name		= N'Angela' ,
	GenderId	= 2;

DECLARE
	@tblLastNames
TABLE
(
	Name NVARCHAR(20) NOT NULL
);

INSERT INTO @tblLastNames
(
	Name
)
SELECT
	Name = N'Jones'

UNION ALL

SELECT
	Name = N'McDonald'

UNION ALL

SELECT
	Name = N'Simon'

UNION ALL

SELECT
	Name = N'Petty'

UNION ALL

SELECT
	Name = N'Bond'

UNION ALL

SELECT
	Name = N'Simpson'

UNION ALL

SELECT
	Name = N'Polsky'

UNION ALL

SELECT
	Name = N'Mayers'

UNION ALL

SELECT
	Name = N'Taylor'

UNION ALL

SELECT
	Name = N'Austin'

UNION ALL

SELECT
	Name = N'Ramsfeld';



INSERT INTO
	dbo.Members
(
	UserName ,
	Password ,
	FirstName ,
	LastName ,
	StreetAddress ,
	CountryId ,
	PhoneNumber ,
	EmailAddress ,
	GenderId ,
	BirthDate ,
	SexualPreferenceId ,
	MaritalStatusId ,
	Picture ,
	RegistrationDateTime
)
SELECT TOP (100000)
	UserName				= REPLICATE (N'X' , ABS (CHECKSUM (NEWID ())) % 10 + 1) ,
	Password				= CAST (ROW_NUMBER () OVER (ORDER BY (SELECT NULL) ASC) AS NVARCHAR(10)) ,
	FirstName				= FirstNames.Name ,
	LastName				= LastNames.Name ,
	StreetAddress			=	CASE
									WHEN ABS (CHECKSUM (NEWID ())) % 100 < 20
										THEN NULL
									ELSE
										REPLICATE (N'X' , ABS (CHECKSUM (NEWID ())) % 100 + 1)
								END ,
	CountryId				= ABS (CHECKSUM (NEWID ())) % 5 + 1 ,
	PhoneNumber				=	CASE
									WHEN ABS (CHECKSUM (NEWID ())) % 100 < 20
										THEN NULL
									ELSE
										N'1234567890'
								END ,
	EmailAddress			= REPLICATE (N'x' , ABS (CHECKSUM (NEWID ())) % 10 + 1) + N'@gmail.com' ,
	GenderId				= FirstNames.GenderId ,
	BirthDate				= CAST (DATEADD (DAY , DATEDIFF (DAY , '1900-01-01' , SYSDATETIME ()) - (19 * 365) - (ABS (CHECKSUM (NEWID ())) % (30 * 365)) , '1900-01-01') AS DATE) ,
	SexualPreferenceId		=	CASE RandomValueTable.RandomValue
									WHEN 1
										THEN 1
									WHEN 2
										THEN 2
									WHEN 3
										THEN NULL
								END ,	
	MaritalStatusId			=	CASE
									WHEN ABS (CHECKSUM (NEWID ())) % 100 < 20
										THEN NULL
									ELSE
										ABS (CHECKSUM (NEWID ())) % 4 + 1
								END ,
	Picture					=	CASE
									WHEN ABS (CHECKSUM (NEWID ())) % 100 < 30
										THEN NULL
									ELSE
										CAST (N'Picture' AS VARBINARY(MAX))
								END ,
	RegistrationDateTime	= SYSDATETIME ()
FROM
	sys.all_columns
CROSS JOIN
	@tblFirstNames AS FirstNames
CROSS JOIN
	@tblLastNames AS LastNames
CROSS JOIN
	(
		SELECT
			RandomValue = ABS (CHECKSUM (NEWID ())) % 3 + 1
	)
	AS
		RandomValueTable
ORDER BY
	NEWID () ASC;
GO