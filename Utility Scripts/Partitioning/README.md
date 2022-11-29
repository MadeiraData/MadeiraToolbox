# Partitions Management for SQL Server

This folder contains several stored procedures and scripts to help you implement
an automated Sliding Window maintenance for your partitioned tables in SQL Server.

## Scripts

The stored procedures (more details on them can be found below):

- [PartitionManagement_Split.sql](PartitionManagement_Split.sql)
- [PartitionManagement_Purge.sql](PartitionManagement_Purge.sql)

There are also a couple more useful scripts within the same folder:

- [Create partition function and partition scheme.sql](Create%20partition%20function%20and%20partition%20scheme.sql) - A template script to create an entirely new partition function and partition scheme
- [Collect Partitioning Information.sql](Collect%20Partitioning%20Information.sql) - A query to output detailed meta-data about any partitions that currently exist in your database

## Remarks

### Considerations when creating new partition ranges

When creating new partition ranges, there are a few assumptions we must consider:

1. We always split the **last** partition range, which is the one with the maximum value.
2. The last partition range must always be empty so that no data movement would be required during the split.
3. To make sure that the last partition range is empty, we should create several "buffer" partition ranges in advance.
4. The partition range intervals should be uniform. This isn't a must, but if they're uniform then it would make partition management much easier.

### Considerations when eliminating old partition ranges

When purging old partition ranges, there are a few assumptions we must consider:

1. We always eliminate the first partition range, which is the one with the minimum value.
2. The first partition range must be empty so that no data movement would be required during the merge.
3. To make sure that the first partition is empty, we truncate the data inside of it before performing the merge.

## The Stored Procedures

### PartitionManagement_Split

This procedure creates new partition ranges based on the specified parameters:

**`@PartitionFunctionName sysname`**

This is the most important parameter. It specifies the name of the partition function that you would want to maintain.
All partition schemes dependent on this function, and in turn, all tables and indexes dependent on these partition schemes, would be automatically affected by the operations performed by this stored procedure.

**`@RoundRobinFileGroups nvarchar(max) = N'PRIMARY'`**

This parameter receives a comma-separated list of filegroup names (one or more), that would work in a round-robin method when creating new partition ranges. For example:

```sql
@RoundRobinFileGroups = 'PRIMARY'
```

The example above would create new partition ranges in the PRIMARY filegroup only.

Another example:

```sql
@RoundRobinFileGroups = 'FG1,FG2,FG3'
```

The above would create new partition ranges based on which filegroup the currently last partition belongs to. If it belongs to FG1, for example, then the next filegroup would be FG2. If it belongs to FG2, then the next filegroup would be FG3. If it belongs to FG3, then the next filegroup would be FG1.

**IMPORTANT REMARKS:**

- The list of filegroups must be distinct. If a filegroup is specified more than once then an error will be raised.
- This logic is applied per each partition scheme that uses the specified partition function. The current version of the **PartitionManagement_Split** procedure does NOT support a level of flexibility where different partition schemes depend on the same partition function, but have inconsistent filegroup mappings (such that cannot be expressed in a round-robin list of filegroups).
- It is possible, however, to have different partition schemes with a different order of filegroup mappings. For example, one partition scheme could map to filegroups *FG1, FG2, FG3, FG1, FG2, ...* while another partition scheme could map to *FG3, FG1, FG2, FG3, FG1, ...* (i.e. the starting filegroup is different but the round-robin list is still identical).

**`@TargetRangeValue sql_variant = NULL`**

You can specify a value for this parameter in order to determine the maximum value that the partition ranges must accommodate.
This is a sql_variant parameter, but its underlying data type must be compatible with the actual partition key data type.

**`@PartitionIncrementExpression nvarchar(4000)`**

You can specify a dynamic SQL expression in this parameter, to determine how each new partition range would be calculated, based on the last range value. For example:

```sql
@PartitionIncrementExpression = N'DATEADD(month, 1, CONVERT(datetime, @CurrentRangeValue))'
```

This will add one month to each new partition range. Note the usage of the variable `@CurrentRangeValue` which is of type `sql_variant`, and therefore must be converted to datetime before being used in the `DATEADD` function. Based on how you want your partition ranges to be created, you could replace "month" with any other date part (day, year, etc.).

Another example:

```sql
@PartitionIncrementExpression = N'CONVERT(bigint, @CurrentRangeValue) + CONVERT(bigint, @PartitionRangeInterval)'
```

This will arithmetically add the last partition range interval to the last partition range in order to calculate the next partition range value. Note again that both specified variables (`@CurrentRangeValue` and `@PartitionRangeInterval`) are of the `sql_variant` data type, and therefore must be explicitly converted before being used in the calculation.

**`@BufferIntervals int = 200`**

If you do not specify a value for `@TargetRangeValue`, it will be calculated instead based on the `@BufferIntervals` parameter. This parameter specifies how many partition intervals should be created in advance relative to partitions that already contain data. In other words, it checks what is the very last partition that contains data, and creates the number of intervals ahead of it based on the `@BufferIntervals` parameter. Its default value is 200. If there are no tables that contain any data in any partition, then no buffer intervals would be created.

**`@PartitionRangeInterval sql_variant = NULL`**

This is an optional parameter that specifies the interval between each partition range. It is used within the expression specified in `@PartitionIncrementExpression`.
If no value is specified for this parameter, then it will be automatically retrieved based on the interval between the last two partition boundaries.

**`@DebugOnly bit = 0`**

When set to 1, this parameter can be used to only print out the generated commands without executing them.

#### Examples

**Example 1: Create monthly partitions one year forward**

```sql
DECLARE @FutureValue datetime = DATEADD(year,1, CONVERT(date, GETDATE()))

EXEC dbo.[PartitionManagement_Split]
	  @PartitionFunctionName = 'MyMonthlyPartitionFunctionName'
	, @RoundRobinFileGroups = 'PRIMARY'
	, @TargetRangeValue = @FutureValue
	, @PartitionIncrementExpression = 'DATEADD(MM, 1, CONVERT(datetime, @CurrentRangeValue))'
	, @DebugOnly = 0
```

**Example 2: Create 200 buffer partitions beyond the current populated value, using the last interval as the increment, and two round-robin filegroups**

```sql
EXEC dbo.[PartitionManagement_Split]
	  @PartitionFunctionName = 'MyPartitionFunctionName'
	, @RoundRobinFileGroups = 'FG_Partitions_1,FG_Partitions_2'
	, @TargetRangeValue = NULL
	, @BufferIntervals = 200
	, @DebugOnly = 0
```

### PartitionManagement_Purge

This procedure can be used to truncate and merge old partitions, in order to implement a retention depth policy.

Its parameters are as follows.

**`@PartitionFunctionName sysname`**

This is the most important parameter. It specifies the name of the partition function that you would want to maintain.
All partition schemes dependent on this function, and in turn, all tables and indexes dependent on these partition schemes, would be automatically affected by the operations in this stored procedure.

**`@MinValueToKeep sql_variant`**

This parameter must be specified in order to determine the minimum value that the partition ranges must accommodate, and therefore - how many partitions should be truncated and merged.

**`@MinPartitionsToKeep int = 3`**

If the total number of partition ranges is equal to or lower than the number specified in this parameter, then no more partitions would be purged.

**`@TruncateOldPartitions bit = 1`**

You can optionally set this parameter to 0 if you don't want old data to be truncated, but let the older partitions be merged anyway. It's strongly advised NOT to set this parameter to 0, as it could cause significant data movement while merging the old partitions.

**`@DebugOnly bit = 0`**

When set to 1, this parameter can be used to only print out the generated commands without executing them.

#### Examples

**Example 1: Purge partitions one year back, based on minimum value to keep**

```sql
DECLARE @MinDateValueToKeep datetime = DATEADD(year, -1, GETDATE())

EXEC dbo.[PartitionManagement_Purge]
	  @PartitionFunctionName = 'MyMonthlyPartitionFunctionName'
	, @MinValueToKeep = @MinDateValueToKeep
	, @TruncateOldPartitions = 1
	, @DebugOnly = 0
```

**Example 2: Purge old partitions and enforce a minimal number of partitions**

```sql
DECLARE @MinDateValueToKeep datetime = DATEADD(year, -1, GETDATE())

EXEC dbo.[PartitionManagement_Purge]
	  @PartitionFunctionName = 'MyMonthlyPartitionFunctionName'
	, @MinValueToKeep = @MinDateValueToKeep
	, @MinPartitionsToKeep = 1000
	, @TruncateOldPartitions = 1
	, @DebugOnly = 0
```


## The Automatic Sliding Window Maintenance

Once you have both stored procedures created in your database, the only thing left to do is **create an automated scheduled job** to run them both per each partition function that you want to maintain.

For more details on how to create an automated scheduled job in SQL Server, go ahead and [read this official documentation](https://docs.microsoft.com/en-us/sql/ssms/agent/create-a-transact-sql-job-step).

The job step commands would be a similar variation of the code examples written above.

For example, you could create a job with two steps:

1. Run PartitionManagement_Split to create new partition ranges.
2. Run PartitionManagement_Purge to purge old partition ranges.
