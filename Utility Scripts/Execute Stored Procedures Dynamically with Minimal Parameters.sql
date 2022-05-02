/*
	Execute Stored Procedures Dynamically with Minimal Parameters
	-------------------------------------------------------------
	
	Copyright Eitan Blumin (c) 2022; email: eitan@madeiradata.com
	You may use the contents of this SQL script or parts of it, modified or otherwise
	for any purpose that you wish (including commercial).
	Under the single condition that you include in the script
	this comment block unchanged, and the URL to the original source, which is:
	http://www.eitanblumin.com/

Example Usage (within a stored procedure):

	DECLARE
		@ProcedureName sysname = QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + N'.' + QUOTENAME(OBJECT_NAME(@@PROCID)),
		@Params xml, @ParsedSQL NVARCHAR(MAX)

	-- Use this syntax to set the procedure parameters as XML
	SET @Params = (
		SELECT
			  [number] = @number
			, [result] = @result
		FOR XML PATH('Parameters')
	)

	EXEC [ProcedureDynamicExecution] @ProcedureName, @Params, @ParsedSQL OUTPUT, @RunCommand = 1
*/
CREATE PROCEDURE [dbo].[ProcedureDynamicExecution]
	  @ProcedureName		NVARCHAR(MAX)	-- the stored procedure name to be executed. must exist within current database.
	, @XmlParams			XML = NULL		-- the XML definition of the parameter values
	, @ParsedSQL			NVARCHAR(MAX) = NULL OUTPUT	-- returns the parsed SQL command to be used for outer sp_executesql.
	, @ProcedureParamsList	NVARCHAR(MAX) = NULL OUTPUT	-- returns the inner procedure parameters to be delivered for the nested execution
	, @InnerParamsInit		NVARCHAR(MAX) = NULL OUTPUT	-- returns the inner parameters declaration
	, @RunCommand			BIT = 1			-- determines whether to run the parsed command (otherwise just output the command w/o running it)
	, @ParametersValidation	BIT = 1		-- set to 0 to skip procedure parameter validation
AS
BEGIN
	SET XACT_ABORT, ARITHABORT, NOCOUNT ON;
	DECLARE @FullProcedureName NVARCHAR(600), @NotValidParameters NVARCHAR(MAX), @MissingParameters NVARCHAR(MAX);
	SET @FullProcedureName = QUOTENAME(OBJECT_SCHEMA_NAME(OBJECT_ID(@ProcedureName))) + N'.' + QUOTENAME(OBJECT_NAME(OBJECT_ID(@ProcedureName)))

	IF @FullProcedureName IS NULL
	BEGIN
		RAISERROR(N'Procedure "%s" not found or not valid for this operation.', 16, 1, @ProcedureName);
		RETURN -1;
	END

	SET @InnerParamsInit = NULL;
	SET @ProcedureParamsList = NULL;

	-- Parse parameters
	;WITH
	ProcedureParameters AS
	(
		SELECT
			[ParamName] = ProcParams.[name],
			[ParamSqlDataType] = QUOTENAME(ParamType.[name])
					+ CASE
						WHEN ParamType.name LIKE '%char' OR ParamType.name LIKE '%binary' THEN N'(' + ISNULL(CONVERT(nvarchar(MAX), NULLIF(ProcParams.max_length,-1)),'max') + N')'
						WHEN ParamType.name IN ('decimal', 'numeric') THEN N'(' + CONVERT(nvarchar(MAX), ProcParams.precision) + N',' + CONVERT(nvarchar(MAX), ProcParams.scale) + N')'
						WHEN ParamType.name IN ('datetime2','time') THEN N'(' + CONVERT(nvarchar(MAX), ProcParams.scale) + N')'
						ELSE N''
					  END
		FROM
			sys.parameters AS ProcParams
		INNER JOIN
			sys.types AS ParamType
		ON
			ProcParams.user_type_id = ParamType.user_type_id
			AND ProcParams.system_type_id = ParamType.system_type_id
		WHERE
			ProcParams.object_id = OBJECT_ID(@ProcedureName)
	),
	ParamValues AS
	(
			SELECT DISTINCT
				ProcParamName = cast(X.query('local-name(.)') as varchar(1000))
			FROM
				@XmlParams.nodes('//Parameters/*') AS T(X)
	)
	SELECT
		  @ProcedureParamsList = ISNULL(@ProcedureParamsList + N', ', N'') + ProcedureParameters.ParamName
		, @InnerParamsInit = ISNULL(@InnerParamsInit, N'') + N'
	DECLARE ' + ProcedureParameters.ParamName + N' ' + ProcedureParameters.[ParamSqlDataType] + N';
			SET ' + ProcedureParameters.ParamName + N' = @XmlParams.value(''(/Parameters/' + ParamValues.ProcParamName + N'/text())[1]'', ''' + ProcedureParameters.[ParamSqlDataType] + N''')'
		, @NotValidParameters =
			STUFF((
				SELECT N', ' + ProcParamName
				FROM
				(
					SELECT ProcParamName FROM ParamValues
					EXCEPT
					SELECT [ParamName] FROM ProcedureParameters
				) AS q
				FOR XML PATH('')
			), 1, 2, N'')
		, @MissingParameters =
			STUFF((
				SELECT N', ' + [ParamName]
				FROM
				(
					SELECT [ParamName] FROM ProcedureParameters
					EXCEPT
					SELECT ProcParamName FROM ParamValues
				) AS q
				FOR XML PATH('')
			), 1, 2, N'')
	FROM
		ParamValues
	INNER JOIN
		ProcedureParameters
	ON
		ProcedureParameters.[ParamName] = N'@' + ParamValues.ProcParamName


	-- Construct the final parsed SQL command
	SET @ParsedSQL = ISNULL(@InnerParamsInit, '') + N'
	EXEC ' + @FullProcedureName + ISNULL(N' ' + @ProcedureParamsList, N'')

	IF @ParametersValidation = 1 AND (@NotValidParameters IS NOT NULL OR @MissingParameters IS NOT NULL)
	BEGIN
		RAISERROR(N'Not valid parameters: %s, Missing Parameters: %s', 16, 1, @NotValidParameters, @MissingParameters);
		RETURN -1;
	END

	-- Optionally run the command
	IF @RunCommand = 1
	BEGIN
		-- TODO: Optionally replace the line below with a special execution
		-- (for example, execute per each database)
		EXEC sp_executesql @ParsedSQL, N'@XmlParams xml', @XmlParams
	END
END