Declare	@Archive Int=0,
		@ArchivesSQL Int,
		@ArchivesAgent Int;

Declare	@xp_enumerrorlogs As Table
		(Archive Int,
		[Date] SmallDateTime,
		LogFileSizeBytes BigInt);

IF		Object_ID('tempdb..#Log','U') Is Not Null
		Drop Table #Log;
Create	Table #Log
		(ID Int Identity Primary Key Clustered,
		Log Varchar(10),
		DT DateTime,
		ProcessInfo Varchar(10),
		Text Varchar(Max));

Alter Table #Log Add Constraint Df_#Log_Log Default 'SQL Server' For [Log];

Insert
Into	@xp_enumerrorlogs(Archive,[Date],LogFileSizeBytes)
EXEC	xp_enumerrorlogs 1;
Set		@ArchivesSQL=@@ROWCOUNT;
While	@Archive<@ArchivesSQL
		Begin
		Insert
		Into	#Log(DT,ProcessInfo,Text)
		Exec	xp_readerrorlog @Archive,1;

		Set		@Archive+=1;
		End

Alter Table #Log Drop Constraint Df_#Log_Log;
Alter Table #Log Add Constraint Df_#Log_Log Default 'Agent' For [Log];
Set		@Archive=0;

Insert
Into	@xp_enumerrorlogs(Archive,[Date],LogFileSizeBytes)
EXEC	xp_enumerrorlogs 2;
Set		@ArchivesAgent=@@ROWCOUNT;
While	@Archive<@ArchivesAgent
		Begin
		Insert
		Into	#Log(DT,ProcessInfo,Text)
		Exec	xp_readerrorlog @Archive,2;

		Set		@Archive+=1;
		End

Select	*
From	#Log
Order By DT;