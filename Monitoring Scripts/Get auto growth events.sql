USE [master]
GO

IF (SELECT CONVERT(INT,value_in_use) FROM sys.configurations WHERE NAME = 'default trace enabled') = 1
BEGIN 
	DECLARE @curr_tracefilename VARCHAR(500);
	DECLARE @base_tracefilename VARCHAR(500);
	DECLARE @indx INT;

	SELECT @curr_tracefilename = path FROM sys.traces WHERE is_default = 1;
	SET @curr_tracefilename = REVERSE(@curr_tracefilename);
	SELECT @indx  = PATINDEX('%\%', @curr_tracefilename) ;
	SET @curr_tracefilename = REVERSE(@curr_tracefilename) ;
	SET @base_tracefilename = LEFT( @curr_tracefilename,LEN(@curr_tracefilename) - @indx) + '\log.trc'; 
	
	SELECT
		--(DENSE_RANK() OVER (ORDER BY StartTime DESC))%2 AS l1,
		ServerName AS [SQL_Instance],
		--CONVERT(INT, EventClass) AS EventClass,
		DatabaseName AS [Database_Name],
		Filename AS [Logical_File_Name],
		(Duration/1000) AS [Duration_MS],
		CONVERT(VARCHAR(50),StartTime, 121) AS [Start_Time],
		--EndTime,
		CAST((IntegerData*8.0/1024) AS DECIMAL(19,2)) AS [Change_In_Size_MB]
	FROM ::fn_trace_gettable(@base_tracefilename, default)
	WHERE 
		EventClass >=  92
		AND EventClass <=  95
		--AND ServerName = @@SERVERNAME
		--AND DatabaseName = 'Tempdb'  
	ORDER BY  StartTime DESC;  --DatabaseName,
END     
ELSE    
	PRINT N'default trace is not enabled!'