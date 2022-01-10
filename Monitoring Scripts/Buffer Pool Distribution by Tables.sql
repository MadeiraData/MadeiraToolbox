/*=============================================================================================================================
-- Description:		This query displays the distribution of data pages in the buffer pool among tables in the current database.
-- Author:			Guy Glantser, Madeira Data Solutions
-- Create Date:		09/01/2022
-- Last Updated On:	09/01/2022
-- Notes:			This query is very useful for understanding and resolving problems related memory utilization
--					and to the Page Life Expectancy counter.
==============================================================================================================================*/

WITH
	PageDistribution
(
	AllocationUnitId ,
	NumberOfPages
)
AS
(
	SELECT
		AllocationUnitId	= allocation_unit_id ,
		NumberOfPages		= COUNT (page_id)
	FROM
		sys.dm_os_buffer_descriptors
	WHERE
		database_id = DB_ID ()
	GROUP BY
		allocation_unit_id
)
SELECT
	SchemaName					= SCHEMA_NAME (Tables.[schema_id]) ,
	TableName					= Tables.[name] ,
	MemorySpace_MB				= CAST (CAST (SUM (PageDistribution.NumberOfPages) AS DECIMAL(19,2)) * 8.0 / 1024.0  AS DECIMAL(19,2)) ,
	TotalTableSpace_MB			= CAST (CAST (SUM (AllocationUnits.used_pages) AS DECIMAL(19,2)) * 8.0 / 1024.0 AS DECIMAL(19,2)) ,
	PercentageOfTableInMemory	= FORMAT (CAST (SUM (PageDistribution.NumberOfPages) AS DECIMAL(19,2)) / CAST (SUM (AllocationUnits.used_pages) AS DECIMAL(19,2)) , 'P')
FROM
	sys.tables AS Tables
INNER JOIN
	sys.partitions AS Partitions
ON
	Tables.[object_id] = Partitions.[object_id]
INNER JOIN
	sys.allocation_units AS AllocationUnits
ON
	AllocationUnits.[type] IN (1,3)
AND
	Partitions.hobt_id = AllocationUnits.container_id
OR
	AllocationUnits.[type] = 2
AND
	Partitions.partition_id = AllocationUnits.container_id
LEFT OUTER JOIN
	PageDistribution
ON
	AllocationUnits.allocation_unit_id = PageDistribution.AllocationUnitId
GROUP BY
	SCHEMA_NAME (Tables.[schema_id]) ,
	Tables.name
HAVING
	SUM (PageDistribution.NumberOfPages) IS NOT NULL
ORDER BY
	MemorySpace_MB DESC;
GO
