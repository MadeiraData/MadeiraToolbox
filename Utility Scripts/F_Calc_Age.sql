CREATE FUNCTION [dbo].[F_Calc_Age](@EventDate  date,@Birthday date)
returns decimal(6,2)
as

begin

       /******************************************
       In case one of the dates is null we will return -1
       *******************************************/
       if (@EventDate is null or @Birthday is null) 
              return -1

       declare @month_gap int;
       declare @Str_Return as decimal(6,2);
       declare @day_gap int;
       declare @year_gap int;     
       

       /***************************************
       This function calculate age according to
       birth date and event date.
       the age is being accurate in years and months.  
       ***************************************/
              
       set @month_gap = month(@Birthday) - month(@EventDate);
       set @day_gap = case when day(@Birthday)  =  day(eomonth(@Birthday)) and day(@EventDate) =  day(eomonth(@EventDate)) then 0
                                      else day(@Birthday)  - day(@EventDate)
                              end;
       set @year_gap = year(@Birthday) - year(@EventDate);
       set @Str_Return = case when @EventDate < @Birthday then 999.00
                              when DATEDIFF(day,@Birthday,@EventDate) > 43800 then 999.00
                              when @year_gap  = 0 and @month_gap = 0 then 0.00
                                            when @month_gap > 0 and @day_gap >0 and day(@EventDate) = day(eomonth(@EventDate)) then abs(cast(datediff(dd,@EventDate,@Birthday) /365.25 as int)) + ((12-@month_gap)*1.00/100.00)   
                              when @month_gap > 0 and @day_gap >0  then abs(cast(datediff(dd,@EventDate,@Birthday) /365.25 as int)) + ((12-@month_gap-1)*1.00/100.00)                                   
                                            when @month_gap > 0 and @day_gap <0 and day(@EventDate) = day(eomonth(@EventDate))  then abs(cast(datediff(dd,@EventDate,@Birthday) /365.25 as int)) + ((12-@month_gap)*1.00/100.00)
                                            when @month_gap > 0 and @day_gap <=0  then abs(cast(datediff(dd,@EventDate,@Birthday) /365.25 as int)) + ((12-@month_gap)*1.00/100.00)
                              when @month_gap = 0 and @day_gap >0   then abs(cast(datediff(dd,@EventDate,@Birthday) /365.25 as int)) + 0.11   
                                            when @month_gap = 0 and @day_gap <0   then floor(abs(datediff(dd,@EventDate,@Birthday) /365.25 ))
                                            when @month_gap = 0 then abs(datediff(dd,@EventDate,@Birthday) /365)*1.00       
                                            
                                            when @month_gap < 0 and @day_gap = 0 then abs(cast(datediff(dd,@EventDate,@Birthday) /365.25 as int)) + ((abs(@month_gap))*1.00/100.00)
                                            when @month_gap < 0 and @day_gap >0 
                                                 and day(@EventDate) = 28
                                                 and ((day(@Birthday)  =  day(eomonth(@Birthday)) and month(@Birthday) = 2) or (day(@EventDate) =  day(eomonth(@EventDate)) and month(@EventDate) = 2))
                                                       then abs(cast(datediff(dd,@EventDate,@Birthday) /365.25 as int)) + (abs(@month_gap))*1.00/100.00
                              when @month_gap < 0 and @day_gap >0 then abs(cast(datediff(dd,@EventDate,@Birthday) /365.25 as int))  + (abs(@month_gap)-1)*1.00/100.00                                       
                                         else  abs(cast(datediff(dd,@EventDate,@Birthday) /365.25 as int))  + (abs(@month_gap))*1.00/100.00 
                                  end    ;                                           
        
   return @Str_Return
    
End
