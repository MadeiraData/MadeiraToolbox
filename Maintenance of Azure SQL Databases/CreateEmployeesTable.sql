DROP TABLE IF EXISTS [dbo].[Employees]
CREATE TABLE [dbo].[Employees](
	[EmployeeID] [int] IDENTITY(1,1) NOT NULL,
	[LastName] [nvarchar](20) NOT NULL,
	[FirstName] [nvarchar](10) NOT NULL,
	[Title] [nvarchar](30) NULL,
	[TitleOfCourtesy] [nvarchar](25) NULL,
	[BirthDate] [datetime] NULL,
	[HireDate] [datetime] NULL,
	[Address] [nvarchar](60) NULL,
	[City] [nvarchar](15) NULL,
	[Region] [nvarchar](15) NULL,
	[PostalCode] [nvarchar](10) NULL,
	[Country] [nvarchar](15) NULL,
	[HomePhone] [nvarchar](24) NULL,
	[Extension] [nvarchar](4) NULL,
	[Photo] [image] NULL,
	[Notes] [ntext] NULL,
	[ReportsTo] [int] NULL,
	[PhotoPath] [nvarchar](255) NULL,
 CONSTRAINT [PK_Employees] PRIMARY KEY CLUSTERED 
(
	[EmployeeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[Employees]  WITH NOCHECK ADD  CONSTRAINT [FK_Employees_Employees] FOREIGN KEY([ReportsTo])
REFERENCES [dbo].[Employees] ([EmployeeID])
GO

ALTER TABLE [dbo].[Employees] CHECK CONSTRAINT [FK_Employees_Employees]
GO

ALTER TABLE [dbo].[Employees]  WITH NOCHECK ADD  CONSTRAINT [CK_Birthdate] CHECK  (([BirthDate] < getdate()))
GO

ALTER TABLE [dbo].[Employees] CHECK CONSTRAINT [CK_Birthdate]
GO

--==============

insert  into [dbo].[Employees]
(
--EmployeeID,
LastName,
FirstName,
Title,
TitleOfCourtesy,
BirthDate,
HireDate,
Address,
City,
Region,
PostalCode,
Country,
HomePhone,
Extension,
Photo,
Notes,
ReportsTo
)
values 
('Davolio','Nancy','Sales Representative','Ms.','1968-12-08 00:00:00','1992-05-01 00:00:00','507 - 20th Ave. E.\r\nApt. 2A','Seattle','WA','98122','USA','(206) 555-9857','5467','nancy.jpg','Education includes a BA in psychology from Colorado State University.  She also completed \"The Art of the Cold Call.\"  Nancy is a member of Toastmasters International.',2),
('Fuller','Andrew','Vice President, Sales','Dr.','1952-02-19 00:00:00','1992-08-14 00:00:00','908 W. Capital Way','Tacoma','WA','98401','USA','(206) 555-9482','3457','andrew.jpg','Andrew received his BTS commercial and a Ph.D. in international marketing from the University of Dallas.  He is fluent in French and Italian and reads German.  He joined the company as a sales representative, was promoted to sales manager and was then named vice president of sales.  Andrew is a member of the Sales Management Roundtable, the Seattle Chamber of Commerce, and the Pacific Rim Importers Association.',NULL),
('Leverling','Janet','Sales Representative','Ms.','1963-08-30 00:00:00','1992-04-01 00:00:00','722 Moss Bay Blvd.','Kirkland','WA','98033','USA','(206) 555-3412','3355','janet.jpg','Janet has a BS degree in chemistry from Boston College).  She has also completed a certificate program in food retailing management.  Janet was hired as a sales associate and was promoted to sales representative.',2),
('Peacock','Margaret','Sales Representative','Mrs.','1958-09-19 00:00:00','1993-05-03 00:00:00','4110 Old Redmond Rd.','Redmond','WA','98052','USA','(206) 555-8122','5176','margaret.jpg','Margaret holds a BA in English literature from Concordia College and an MA from the American Institute of Culinary Arts. She was temporarily assigned to the London office before returning to her permanent post in Seattle.',2),
('Buchanan','Steven','Sales Manager','Mr.','1955-03-04 00:00:00','1993-10-17 00:00:00','14 Garrett Hill','London','','SW1 8JR','UK','(71) 555-4848','3453','steven.jpg','Steven Buchanan graduated from St. Andrews University, Scotland, with a BSC degree.  Upon joining the company as a sales representative, he spent 6 months in an orientation program at the Seattle office and then returned to his permanent post in London, where he was promoted to sales manager.  Mr. Buchanan has completed the courses \"Successful Telemarketing\" and \"International Sales Management.\"  He is fluent in French.',2),
('Suyama','Michael','Sales Representative','Mr.','1963-07-02 00:00:00','1993-10-17 00:00:00','Coventry House\r\nMiner Rd.','London','','EC2 7JR','UK','(71) 555-7773','428','michael.jpg','Michael is a graduate of Sussex University (MA, economics) and the University of California at Los Angeles (MBA, marketing).  He has also taken the courses \"Multi-Cultural Selling\" and \"Time Management for the Sales Professional.\"  He is fluent in Japanese and can read and write French, Portuguese, and Spanish.',5),
('King','Robert','Sales Representative','Mr.','1960-05-29 00:00:00','1994-01-02 00:00:00','Edgeham Hollow\r\nWinchester Way','London','','RG1 9SP','UK','(71) 555-5598','465','robert.jpg','Robert King served in the Peace Corps and traveled extensively before completing his degree in English at the University of Michigan and then joining the company.  After completing a course entitled \"Selling in Europe,\" he was transferred to the London office.',5),
('Callahan','Laura','Inside Sales Coordinator','Ms.','1958-01-09 00:00:00','1994-03-05 00:00:00','4726 - 11th Ave. N.E.','Seattle','WA','98105','USA','(206) 555-1189','2344','laura.jpg','Laura received a BA in psychology from the University of Washington.  She has also completed a course in business French.  She reads and writes French.',2),
('Dodsworth','Anne','Sales Representative','Ms.','1969-07-02 00:00:00','1994-11-15 00:00:00','7 Houndstooth Rd.','London','','WG2 7LT','UK','(71) 555-4444','452','anne.jpg','Anne has a BA degree in English from St. Lawrence College.  She is fluent in French and German.',5)
;
