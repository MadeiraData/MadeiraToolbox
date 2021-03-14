# Compare SQL Server Instance Properties

- Author: Eitan Blumin
- Creation Date: 2018-06-26

## Instructions

1. You run the first script [GenerateInstancePropertiesForCompare.sql](GenerateInstancePropertiesForCompare.sql) on each SQL Server instance that you need to compare.
2. Right-click on the results and choose "Save Results As..." to save it as a CSV.
3. Copy the output files to some central location (also with a SQL Server) where you'd want to run the comparison.
4. Open the second script [CompareInstanceProperties.sql](CompareInstanceProperties.sql) on the central location, change all the file paths and server names accordingly.
5. Run the script (recommended in TEMPDB) which would use BULK INSERT into a table and then run the comparisons.
6. Profit!

## Possible usages for this script:

- Comparing between two instances that are about to have DB Mirroring or AlwaysOn established between them.
- Comparing between two instances that should be identical due to a business requirement (for example: Making sure sharding instances are identical, or that different on-premise instances are identical, or that a QA instance is identical to a Production instance).
- Comparing between two instances in order to try and find out why one instance performs differently from another (i.e. maybe it's an instance configuration? Or a database setting? Or a hardware difference? etc.).
