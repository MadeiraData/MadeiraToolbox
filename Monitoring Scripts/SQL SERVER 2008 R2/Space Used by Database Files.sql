/*========================================================================================================================

Description:	Retrieve data about size and space used for all the files in the current database
Scope:			Database
Author:			Guy Glantser
Created:		15/02/2013
Last Updated:	15/02/2013
Notes:			Make sure there is enough unallocated space for future growth.
				If the value of "UnallocatedSpace_Percent" is low, consider allocating more space
				on the next maintenance window.
				Auto-growth should be enabled as a safety measure.
				Do not attempt to shrink files, unless you really have to.
				See this post for more information: http://www.madeira.co.il/when-and-how-to-shrink-your-database/

=========================================================================================================================*/


SELECT
	FilegroupName				= Filegroups.name ,
	FileLogicalName				= DatabaseFiles.name ,
	FilePhysicalName			= DatabaseFiles.physical_name ,
	FileSize_MB					= CAST ((CAST (DatabaseFiles.size AS DECIMAL(19,2)) * 8.0 / 1024.0) AS DECIMAL(19,2)) ,
	AllocatedSpace_MB			= CAST ((CAST (FILEPROPERTY (DatabaseFiles.name , 'SpaceUsed') AS DECIMAL(19,2)) * 8.0 / 1024.0) AS DECIMAL(19,2)) ,
	UnallocatedSpace_MB			= CAST ((CAST ((DatabaseFiles.size - FILEPROPERTY (DatabaseFiles.name , 'SpaceUsed')) AS DECIMAL(19,2)) * 8.0 / 1024.0) AS DECIMAL(19,2)) ,
	UnallocatedSpace_Percent	= CAST ((CAST ((DatabaseFiles.size - FILEPROPERTY (DatabaseFiles.name , 'SpaceUsed')) AS DECIMAL(19,2)) / CAST (DatabaseFiles.size AS DECIMAL(19,2)) * 100.0) AS DECIMAL(19,2)) ,
	AutoGrowthType				=
		CASE DatabaseFiles.growth
			WHEN 0
				THEN NULL
			ELSE
				CASE DatabaseFiles.is_percent_growth
					WHEN 0
						THEN N'Megabytes'
					WHEN 1
						THEN N'Percent'
				END
		END ,
	AutoGrowthValue				=
		CASE DatabaseFiles.growth
			WHEN 0
				THEN NULL
			ELSE
				CASE DatabaseFiles.is_percent_growth
					WHEN 0
						THEN CAST ((CAST (DatabaseFiles.growth AS DECIMAL(19,2)) * 8.0 / 1024.0) AS DECIMAL(19,2))
					WHEN 1
						THEN CAST (DatabaseFiles.growth AS DECIMAL(19,2))
				END
		END ,
	FileMaxSize_MB				=
		CASE DatabaseFiles.max_size
			WHEN 0
				THEN NULL
			WHEN -1
				THEN -1
			WHEN 268435456
				THEN -1
			ELSE
				CAST (ROUND ((CAST (DatabaseFiles.max_size AS DECIMAL(19,2)) * 8.0 / 1024.0) , 0) AS INT)
		END ,
	IsReadOnly					= DatabaseFiles.is_read_only
FROM
	sys.database_files AS DatabaseFiles
LEFT OUTER JOIN
	sys.filegroups AS Filegroups
ON
	DatabaseFiles.data_space_id = Filegroups.data_space_id
GO
