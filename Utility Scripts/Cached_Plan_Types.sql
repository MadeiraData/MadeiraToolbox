/*=============================================================================================================

Query: Cached_Plans_Types

Author: Eric Rouach, Madeira Data Solutions - July 2022

Description: The following script will display info about the different types of execution plans
found in the plan cache.

The result set will displau 4 columns:

[Plan_Type] (Adhoc, Proc...)
[Plans_Count] (the number of such plans found in the plan cache)
[Plan_Size_In_GB] (the total size of such plans found in the plan cache in GB)
[Cached_Plans_Ratio] (the percentge of such plans out of the total number of plans found in the plan cache)

The result of this query might help you in deciding whether or not enabling the "Optimize for ad hoc workloads"
advanced server option.

=============================================================================================================*/

	WITH Cached_Plan_Types
		(
			[1],
			[Plan_Type],
			[Plans_Count],
			[Plan_Size_In_MB]
		)
	AS
		(
			SELECT
				1 as [1], 
				objtype AS [Plan_Type],
				COUNT_BIG(1) AS [Plans_Count],
				SUM(CAST(size_in_bytes AS DECIMAL(18, 2))) / 1024 / 1024 AS [Plan_Size_In_MB]
			FROM
				sys.dm_exec_cached_plans 
			WHERE 
				usecounts = 1
			GROUP BY
				objtype
		)
	SELECT
		[Plan_Type],
		[Plans_Count],
		CAST([Plan_Size_In_MB] / 1000 AS DECIMAL(18,2)) AS Plan_Size_In_GB,
		FORMAT(
			  CAST([Plans_Count] AS DECIMAL(18,2))
			  /
			  SUM(CAST([Plans_Count] AS DECIMAL(18,2))) OVER(PARTITION BY [1])
			  ,
			  'P'
			  ) AS Cached_Plans_Ratio
	FROM
		Cached_Plan_Types
	ORDER BY
	    CAST([Plans_Count] AS DECIMAL(18,2))
	    /
	    SUM(CAST([Plans_Count] AS DECIMAL(18,2))) OVER(PARTITION BY [1]) DESC