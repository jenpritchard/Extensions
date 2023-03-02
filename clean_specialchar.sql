CREATE FUNCTION [dbo].[CleanUpSpecialCharacters](
@InputString nvarchar(max),
@ReplaceWith nvarchar(50) = N'',
@RemoveExcelErrors bit = 1)
RETURNS nvarchar(max)
AS
BEGIN


    DECLARE @CleanString nvarchar(max)

set @inputstring ='	Hawaii Standard Time'
set @ReplaceWith = N''
set @RemoveExcelErrors=0

    SET @CleanString = @InputString
    -- Create a temporary table to store the list of special characters and their text names
	DROP TABLE #SpecialCharacters
    CREATE TABLE #SpecialCharacters (
        [Character] nvarchar(10),
        CharacterName nvarchar(100),
		IsDefault bit,
        IsExcelError bit
    )

    -- Insert the list of special characters and their text names into the temporary table
    INSERT INTO #SpecialCharacters (Character, CharacterName, IsDefault,IsExcelError)
    --can't use 'insert into...values' because it will give error "Insert values statement can contain only constant literal values or variable references"
		SELECT  NCHAR(11), N'Vertical Tab', 1, 0 UNION ALL
		SELECT  NCHAR(12), N'Form Feed', 1, 0 UNION ALL
		SELECT  NCHAR(9), N'Tab', 1, 0 UNION ALL
		SELECT  NCHAR(10), N'Line Feed', 1, 0 UNION ALL
		SELECT  NCHAR(13), N'Carriage Return', 1, 0 UNION ALL
		SELECT  NCHAR(160), N'Non-Breaking Space', 1, 0 UNION ALL
		--SELECT  NCHAR(8194), N'En Space', 1, 0 UNION ALL
		--SELECT  NCHAR(8195), N'Em Space', 1, 0 UNION ALL
		SELECT  NCHAR(8201), N'Thin Space', 1, 0 UNION ALL
		SELECT  NCHAR(8204), N'Zero Width Non-Joiner', 1, 0 UNION ALL
		SELECT  NCHAR(8205), N'Zero Width Joiner', 1, 0 UNION ALL
		SELECT  NCHAR(8206), N'Left-to-Right Mark', 1, 0 UNION ALL
		SELECT  NCHAR(8207), N'Right-to-Left Mark', 1, 0 UNION ALL
		SELECT  N'#N/A', N'Value not available', 0,1 UNION ALL
		SELECT  N'#VALUE!', N'Wrong type of argument or operand', 0,1 UNION ALL
		SELECT  N'#REF!', N'Invalid cell reference', 0,1 UNION ALL
		SELECT  N'#NAME?', N'Undefined name', 0,1 UNION ALL
		SELECT  N'#DIV/0!', N'Division by zero', 0,1 UNION ALL
		SELECT  N'#NUM!', N'Number error', 0,1 UNION ALL
		SELECT  N'#NULL!', N'Intersection of two ranges is empty', 0,1

    -- Replace the special characters with the specified string in @ReplaceWith,
/*    -- filtering out the Excel error values based on the value of @RemoveExcelErrors
    SELECT @CleanString = REPLACE(@CleanString, Character, @ReplaceWith)
    FROM #SpecialCharacters
    WHERE IsDefault=1
		AND @RemoveExcelErrors & IsExcelError =1  --the parameter is turned on, and is flagged in the table

select @cleanstring
*/

drop table #temp_lookfor
SELECT *
INTO #temp_lookfor
FROM  #SpecialCharacters
WHERE IsDefault=1
		OR @RemoveExcelErrors & IsExcelError =1


DECLARE @maxrow AS INT 
SELECT @maxrow=count(1) FROM #temp_lookfor


DECLARE @offset AS INT=0
		,@fetch AS INT=1
		,@currentchar AS varchar(10)
WHILE @offset<=@maxrow
BEGIN
	

	
	--Synapse is stupid and can't do a real offset/fetch. Could pare this down, but want to keep the general code here
	--in case I want to use it for something else later
SELECT @currentchar=[Character] FROM
	(SELECT TOP(@fetch) *  FROM ( 
		SELECT TOP (@offset+@fetch) * FROM #temp_lookfor ORDER BY [CharacterName] ASC ) a 
	ORDER BY [CharacterName] DESC
	) b ORDER BY  [CharacterName] ASC
	
	SELECT @CleanString = REPLACE(@CleanString, @currentchar, @ReplaceWith)


	SET @offset=@offset+1
END

 RETURN TRIM(@cleanstring)

END