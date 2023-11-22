/*********************************************
**
** FUNCTION NAME: [dbo].[udf_CapitalizeWords]
** AUTHOR: Jen Pritchard
** CREATION DATE: 2023-04-06
** PURPOSE: 
		Takes a raw string input (up to 4000 characters) and formats it so that the beginning of each word is capitalized
		while all other letters in the word are lower case. Any punctuation or spaces are maintained

** INPUTS:
**		@inputString NVARCHAR(4000): The raw string input to be formatted.

** EXAMPLES:
**	-- Example 1: Format a string with mixed capitalization
**		SELECT dbo.udf_CapitalizeWords('SAmpLe word!') AS CleanedString;
**		-- Expected output: 'Sample Word!'
**
** -- Example 2: Clean an empty string.
**		SELECT dbo.udf_CleanString('') AS CleanedString;
**		-- Expected output: ''
**
** KNOWN LIMITATIONS/BUGS:
** None
**------------------------------------------------------
** FUTURE ENHANCEMENTS:
**		Error trapping

** CHANGE HISTORY:
-------------|------------------|------------------
	Date		Author				Change
------------|-------------------|------------------
**	20230406	J Pritchard			Initial Dev

**********************************************/




if exists (
	select * from sys.objects as o
		join sys.schemas as s on o.schema_id=s.schema_id
	where o.type='IF' and o.name like 'udf_CapitalizeWords' and s.name like 'dbo')
BEGIN
DROP FUNCTION dbo.udf_CapitalizeWords
END
GO

-------------------------------------------------------------------

CREATE FUNCTION dbo.udf_CapitalizeWords (@inputString NVARCHAR(4000))
RETURNS TABLE
AS
RETURN
   (

    SELECT	STRING_AGG(
				CASE WHEN LEN(value) =0 THEN ''
					ELSE UPPER(LEFT(value, 1)) + LOWER(SUBSTRING(value, 2, LEN(value) - 1)) 
					END
				, ' ') AS Capitalized
    FROM STRING_SPLIT(@inputString, ' ')
	)



