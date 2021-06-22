USE [YourDBname]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [YourSchemaName].[DDLEventsAudit]
					(
						[Date]				DATETIME2(7)	NOT NULL DEFAULT SYSDATETIME() PRIMARY KEY,
						[Type]				NVARCHAR(128)	NULL,
						[TSQLCommand]		NVARCHAR(MAX)	NULL,
						[Database]			NVARCHAR(255)	NULL,
						[Schema]			NVARCHAR(255)	NULL,
						[Object]			NVARCHAR(255)	NULL,
						[ByLogin]			NVARCHAR(255)	NULL,
						[Program]			NVARCHAR(255)	NULL,
						[FromHost]			VARCHAR(64)		NULL,
						[IPAddress]			VARCHAR(32)		NULL,
						[EventXML]			XML				NULL
					)
	ON [PRIMARY]
GO
