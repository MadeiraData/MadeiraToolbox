/*
Based on blog post published by Remus Rusanu:
https://rusanu.com/2010/03/26/using-tables-as-queues/
*/
go
create table dbo.TableQueue (
  Id bigint not null identity(1,1) constraint PK_TableQueue primary key clustered,
  Payload varbinary(max) not null
);
go

create procedure dbo.usp_Enqueue
  @payload varbinary(max)
as
  set nocount on;
  insert into dbo.TableQueue (Payload) values (@Payload);
go

create procedure dbo.usp_Dequeue
	@timeOutSeconds int = 20,
	@maxItems int = 1,
	@delayBetweenAttempts varchar(15) = '00:00:00.1'
as
  set nocount on;
  declare @StartTime datetime;
  declare @Output as table (Payload varbinary(max));
  set @StartTime = getdate();

  while dateadd(ss, @timeOutSeconds, @StartTime) >= getdate()
  begin
	;with cte as (
	    select top(@maxItems) Payload
	      from dbo.TableQueue with (rowlock, readpast)
	    order by Id asc)
	  delete from cte
	    output deleted.Payload
	    into @Output(Payload);

	if @@rowcount > 0
		break;
	else if @delayBetweenAttempts is not null
		waitfor delay @delayBetweenAttempts;
  end

  select Payload from @Output;
go
