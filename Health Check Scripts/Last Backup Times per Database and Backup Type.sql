/*========================================================================================================================

Description:	Display the last date & time of each backup type for each database
Scope:			Instance
Author:			Guy Glantser
Created:		03/10/2013
Last Updated:	03/10/2013
Notes:			Make sure there is an adequate backup strategy for each database

=========================================================================================================================*/

SELECT
	DatabaseName							= DatabaseName ,
	LastFullBackupDateTime					= D ,
	LastDifferentialBackupDateTime			= I ,
	LastLogBackupDateTime					= L ,
	LastFileOrFilegroupBackupDateTime		= F ,
	LastDifferentialFileBackupDateTime		= G ,
	LastPartialBackupDateTime				= P ,
	LastDifferentialPartialBackupDateTime	= Q
FROM
	(
		SELECT
			DatabaseId			= Databases.database_id ,
			DatabaseName		= Databases.name ,
			BackupType			= Backups.type ,
			LastBackupDateTime	= MAX (Backups.backup_start_date)
		FROM
			sys.databases AS Databases
		LEFT OUTER JOIN
			msdb.dbo.backupset AS Backups
		ON
			Databases.name = Backups.database_name
		GROUP BY
			Databases.database_id ,
			Databases.name ,
			Backups.type
	)
	AS
		LastBackups
	PIVOT
	(
		MIN (LastBackupDateTime)
	FOR
		BackupType
	IN
		(D , I , L , F , G , P , Q)
	)
	AS
		DatabaseLastBackups
ORDER BY
	DatabaseId ASC;
GO
