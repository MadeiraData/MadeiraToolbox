CREATE FUNCTION [dbo].[F_StringWithNumericAndAlpha](@input_string  nvarchar(max))
RETURNS NVARCHAR(MAX)
AS

BEGIN
       /*************************************************
	   The Goal Of This Function Is to take string which 
	   combune letters in hebrew and numbers
	   and revers them.
	   This haapanes when we get string in flat files.
       **************************************************/
       DECLARE @str nvarchar(max) = @input_string;
       DECLARE @ReturnStr nvarchar(max) = '';
       DECLARE @StrLen int = len(ltrim(rtrim(@str)));
       DECLARE @Index int = 1;
       DECLARE @NumericStr nvarchar(max) = '';
       DECLARE @PrevChar char(1);
       DECLARE @CurrChar char(1);
       DECLARE @NextChar char(1);
       DECLARE @AlphaStr nvarchar(max) = '';

       DECLARE @Special_Char_Ind int = case when patindex('%/%',@str)> 1 then 1 else 0 end;
       DECLARE @Special_Char_Index int = patindex('%/%',@str);
       DECLARE @StrLeftToSpecial nvarchar(max) = case when @Special_Char_Ind > 0 then substring(@str,1,@Special_Char_Index-1) else '' end;
       DECLARE @StrRightToSpecial nvarchar(max) = case when @Special_Char_Ind > 0 then substring(@str,@Special_Char_Index+1,@StrLen) else '' end;
       DECLARE @ReturnLeft nvarchar(max) = '';
       DECLARE @ReturnRight nvarchar(max) = '';

       IF @str IS NULL
              RETURN NULL

       IF @Special_Char_Ind = 0 
              begin
       
                     
                     WHILE @Index <= @StrLen
                           BEGIN
                               SET @PrevChar = @CurrChar;
                               SET @CurrChar = substring(@Str,@Index,1)
                                  SET @NextChar = substring(@Str,@Index+1,1)

                                  SET @NumericStr = case when isnumeric(@CurrChar) = 0 then '' else @NumericStr end
                            
                                  SET @NumericStr += case when isnumeric(@CurrChar) = 1 then @CurrChar else '' end
                                  
                                  SET @AlphaStr   = case when isnumeric(@CurrChar) = 0 then @CurrChar  else '' end
                                  
                                  SET @ReturnStr  += case when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 0 then @AlphaStr
                                                          when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and isnumeric(@NextChar) = 1 and @CurrChar in ('.','+','-') then @NumericStr
                                                          when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 1 and isnumeric(@NextChar) = 0 then REVERSE(@NumericStr) + @AlphaStr
                                                          when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 0 and (isnumeric(@NextChar) = 0 or @NextChar in ('.','+','-')) then REVERSE(@NumericStr) + @AlphaStr
                                                                         when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 1 and @Index = @StrLen then REVERSE(@NumericStr) --> הגענו לסוף המחרוזת
                                                                           when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 1 and @Index < @StrLen then ''                                                        
                                                                           when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and /*@PrevChar<> ' ' and*/ (isnumeric(@NextChar) = 0 or @Index = @StrLen) then @NumericStr
                                                                           when isnumeric(@CurrChar) = 0 then @AlphaStr
                                                                           --when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and @PrevChar = ' ' and (isnumeric(@NextChar) = 0 or @Index = @StrLen) then @NumericStr
                                                                           else ''
                                                                 end
                                  
                                  SET @index += 1;
                                  set @NumericStr = case when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and isnumeric(@NextChar) = 1 and @CurrChar in ('.','+','-') then ''
                                                                       else @NumericStr
                                                                end;
                     
                           END
           end
              ELSE 
                     BEGIN 
                     
                     SET @StrLen = len(ltrim(rtrim(@StrLeftToSpecial)));
                     SET @Str = @StrLeftToSpecial;
                     
                     WHILE @Index <= @StrLen
                           BEGIN
                               SET @PrevChar = @CurrChar;
                               SET @CurrChar = substring(@Str,@Index,1)
                                  SET @NextChar = substring(@Str,@Index+1,1)

                                  SET @NumericStr = case when isnumeric(@CurrChar) = 0 then '' else @NumericStr end
                             
                                  SET @NumericStr += case when isnumeric(@CurrChar) = 1 then @CurrChar else '' end
                                  
                                  SET @AlphaStr   = case when isnumeric(@CurrChar) = 0 then @CurrChar  else '' end
                                  
                                  SET @ReturnLeft  += case when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 0 then @AlphaStr
                                                          when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and isnumeric(@NextChar) = 1 and @CurrChar in ('.','+','-') then @NumericStr
                                                          when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 1 and isnumeric(@NextChar) = 0 then REVERSE(@NumericStr) + @AlphaStr
                                                          when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 0 and (isnumeric(@NextChar) = 0 or @NextChar in ('.','+','-')) then REVERSE(@NumericStr) + @AlphaStr
                                                                         when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 1 and @Index = @StrLen then REVERSE(@NumericStr) --> הגענו לסוף המחרוזת
                                                                           when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 1 and @Index < @StrLen then ''                                                        
                                                                           when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and /*@PrevChar<> ' ' and*/ (isnumeric(@NextChar) = 0 or @Index = @StrLen) then @NumericStr
                                                                           when isnumeric(@CurrChar) = 0 then @AlphaStr
                                                                           --when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and @PrevChar = ' ' and (isnumeric(@NextChar) = 0 or @Index = @StrLen) then @NumericStr
                                                                           else ''
                                                                 end
                                  
                                  SET @index += 1;
                                  set @NumericStr = case when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and isnumeric(@NextChar) = 1 and @CurrChar in ('.','+','-') then ''
                                                                       else @NumericStr
                                                                end;
                                                              
                     
                           END
                           
                           SET @StrLen = len(ltrim(rtrim(@StrRightToSpecial)))+1;
                         SET @Str = @StrRightToSpecial;
                         SET @Index = 1
                           SET @NumericStr = ''
                           WHILE @Index <= @StrLen
                           BEGIN
                               SET @PrevChar = @CurrChar;
                               SET @CurrChar = substring(@Str,@Index,1)
                                  SET @NextChar = substring(@Str,@Index+1,1)

                                  SET @NumericStr = case when isnumeric(@CurrChar) = 0 then '' else @NumericStr end
                             
                                  SET @NumericStr += case when isnumeric(@CurrChar) = 1 then @CurrChar else '' end
                                  
                                  SET @AlphaStr   = case when isnumeric(@CurrChar) = 0 then @CurrChar  else '' end
                                  
                                  SET @ReturnRight  += case when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 0 then @AlphaStr
                                                          when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and isnumeric(@NextChar) = 1 and @CurrChar in ('.','+','-') then @NumericStr
                                                          when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 1 and isnumeric(@NextChar) = 0 then REVERSE(@NumericStr) + @AlphaStr
                                                          when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 0 and (isnumeric(@NextChar) = 0 or @NextChar in ('.','+','-')) then REVERSE(@NumericStr) + @AlphaStr
                                                                         when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 1 and @Index = @StrLen then REVERSE(@NumericStr) --> הגענו לסוף המחרוזת
                                                                           when isnumeric(@PrevChar) = 1 and isnumeric(@CurrChar) = 1 and @Index < @StrLen then ''                                                        
                                                                           when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and /*@PrevChar<> ' ' and*/ (isnumeric(@NextChar) = 0 or @Index = @StrLen) then @NumericStr
                                                                           when isnumeric(@CurrChar) = 0 then @AlphaStr
                                                                           --when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and @PrevChar = ' ' and (isnumeric(@NextChar) = 0 or @Index = @StrLen) then @NumericStr
                                                                           else ''
                                                                 end
                                  
                                  SET @index += 1;
                                  set @NumericStr = case when isnumeric(@PrevChar) = 0 and isnumeric(@CurrChar) = 1 and isnumeric(@NextChar) = 1 and @CurrChar in ('.','+','-') then ''
                                                                       else @NumericStr
                                                                end;
                                                              
                     
                           END

                           set @ReturnStr = @ReturnRight + '/' + ltrim(rtrim(@ReturnLeft))

                     END
                     
                     
                     
       return @ReturnStr
END
