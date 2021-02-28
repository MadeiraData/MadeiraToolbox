if object_id('dbo.SearchCascadeByFK', 'P') is null
	exec(N'CREATE PROC dbo.SearchCascadeByFK AS RETURN;')
go
--exec dbo.SearchCascadeByFK 'dbo.Catalog', @debug = 1 --two part naming convention 
ALTER PROC dbo.SearchCascadeByFK
  @table sysname -- use two part name convention
, @lvl int=0 -- do not change
, @ParentTable sysname=null -- do not change
, @debug bit = 0
as
begin
	SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
	if object_id('tempdb..#tbl') is null create table  #tbl  (id int identity, tableid int, lvl int, ParentTableId int null);

	declare @curS cursor;
	if @lvl = 0
		insert into #tbl (tableid, lvl, ParentTableId) select OBJECT_ID(@table), @lvl, Null;
	else
		insert into #tbl (tableid, lvl, ParentTableId) select OBJECT_ID(@table), @lvl, OBJECT_ID(@ParentTable);

	if @debug=1 print replicate('|----', @lvl) + 'lvl ' + cast(@lvl as varchar(10)) + ' = ' + @table;
	
	if not exists (select * from sys.foreign_keys where referenced_object_id = object_id(@table) and referenced_object_id <> parent_object_id)
		return;
	else
	begin -- else
		set @ParentTable = @table;
		set @curS = cursor for
		select tablename=quotename(object_schema_name(parent_object_id))+N'.'+quotename(object_name(parent_object_id))
		from sys.foreign_keys 
		where referenced_object_id = object_id(@table)
		and parent_object_id <> referenced_object_id; -- add this to prevent self-referencing which can create a indefinitive loop;

		open @curS;
		fetch next from @curS into @table;

		while @@fetch_status = 0
		begin --while
			set @lvl = @lvl+1;
			-- recursive call
			exec dbo.SearchCascadeByFK @table, @lvl, @ParentTable, @debug;
			set @lvl = @lvl-1;
			fetch next from @curS into @table;
		end --while
		close @curS;
		deallocate @curS;
	end -- else
	if @lvl = 0
		select * from #tbl;
	return;
end
go
if object_id('dbo.DeleteCascadeByFK', 'P') is null
	exec(N'CREATE PROC dbo.DeleteCascadeByFK AS RETURN;')
go
/*
declare @cmd nvarchar(max)
exec dbo.DeleteCascadeByFK 'dbo.Catalog', 'where [dbo].[Catalog].[ItemID]=NEWID()', @debug = 1, @executeCmd = 0, @resultCmd = @cmd output

select @cmd as deleteCmd
*/
ALTER PROC dbo.DeleteCascadeByFK
	@table sysname,
	@where nvarchar(max), -- 'where Catalog.id=2'
	@executeCmd bit = 0,
	@resultCmd nvarchar(max) = null output,
	@debug bit = 0
AS
SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
if object_id('tempdb..#tmp') is not null drop table #tmp;
create table  #tmp  (
			id int
			, tableid int
			, lvl int
			, ParentTableId int null
			);

insert into #tmp (id, tableid, lvl, ParentTableId)
exec dbo.SearchCascadeByFK @table=@table, @debug=@debug;

set @resultCmd = N''
declare @curFK cursor, @fk_object_id int;
declare @sqlcmd nvarchar(max)='', @crlf nvarchar(max)=char(0x0d)+char(0x0a);
declare @child sysname, @parent sysname, @lvl int, @id int;
declare @i int;
declare @t table (tablename sysname);
declare @curT cursor;
if isnull(@where, '') = ''
begin
	RAISERROR(N'Unfiltered deletion is not allowed!', 16, 1);
	RETURN;

	--set @curT = cursor for select tablename, lvl from #tmp order by lvl desc
	--open @curT;
	--fetch next from @curT into @child, @lvl;
	--while @@fetch_status = 0
	--begin -- loop @curT
	--	if not exists (select 1 from @t where tablename=@child)
	--		insert into @t (tablename) values (@child);
	--	fetch next from @curT into @child, @lvl;
	--end -- loop @curT
	--close @curT;
	--deallocate @curT;

	--select  @sqlcmd = @sqlcmd + 'delete from ' + tablename + @crlf from @t ;
	--SET @resultCmd = @resultCmd + @crlf + @sqlcmd;
end
else
begin 
	declare curT cursor for
	select  lvl, id
	from #tmp
	order by lvl desc;

	open curT;
	fetch next from curT into  @lvl, @id;
	while @@FETCH_STATUS =0
	begin
		set @i=0;
		if @lvl =0
		begin -- this is the root level
			select @sqlcmd = 'delete from ' + (quotename(object_schema_name(tableid))+N'.'+quotename(object_name(tableid))) from #tmp where id = @id;
		end -- this is the roolt level

		while @i < @lvl
		begin -- while

			select top 1 @child=(quotename(object_schema_name(tableid))+N'.'+quotename(object_name(tableid))), @parent=(quotename(object_schema_name(ParentTableId))+N'.'+quotename(object_name(ParentTableId))) from #tmp where id <= @id-@i and lvl <= @lvl-@i order by lvl desc, id desc;
			set @curFK = cursor for
			select object_id from sys.foreign_keys 
			where parent_object_id = object_id(@child)
			and referenced_object_id = object_id(@parent)

			open @curFK;
			fetch next from @curFk into @fk_object_id
			while @@fetch_status =0
			begin -- @curFK

				if @i=0
					set @sqlcmd = 'delete from ' + @child + @crlf +
					'from ' + @child + @crlf + 'inner join ' + @parent  ;
				else
					set @sqlcmd = @sqlcmd + @crlf + 'inner join ' + @parent ;

				;with c as 
				(
					select child = quotename(object_schema_name(fc.parent_object_id))+N'.' + quotename(object_name(fc.parent_object_id)), child_col=quotename(c.name)
					, parent = quotename(object_schema_name(fc.referenced_object_id))+N'.' + quotename(object_name(fc.referenced_object_id)), parent_col=quotename(c2.name)
					, rnk = row_number() over (order by (select null))
					from sys.foreign_key_columns fc
					inner join sys.columns c
					on fc.parent_column_id = c.column_id
					and fc.parent_object_id = c.object_id
					inner join sys.columns c2
					on fc.referenced_column_id = c2.column_id
					and fc.referenced_object_id = c2.object_id
					where fc.constraint_object_id=@fk_object_id
				)
					select @sqlcmd =@sqlcmd +  case rnk when 1 then ' on '  else ' and ' end 
					+ @child +'.'+ child_col +'='  +  @parent   +'.' + parent_col
					from c;
					fetch next from @curFK into @fk_object_id;
			end --@curFK
			close @curFK;
			deallocate @curFK;
			set @i = @i +1;
		end --while
		SET @resultCmd = @resultCmd + ISNULL(@crlf + @sqlcmd + @crlf + @where + ';' + @crlf, '');
		fetch next from curT into  @lvl, @id;
	end
	close curT;
	deallocate curT;
end

IF @debug = 1 RAISERROR(N'-- Result Command:%s',0,1) WITH NOWAIT;
IF @executeCmd = 1 AND @resultCmd <> N''
	exec(@resultCmd);
go
