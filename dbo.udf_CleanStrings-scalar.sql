/*********************************************
**
** FUNCTION NAME: [dbo].[udf_CleanString]
** AUTHOR: Jen Pritchard
** CREATION DATE: 2023-04-05
** PURPOSE: 
		Takes a raw string input (up to 4000 characters) and replaces any non-printable characters 
		(defined as ASCII value less than 32 or greater than 126) with a specified replacement character (default is an empty string).
		It also removes any extra spaces (definied as trailing/leader or any that are more than 1 in a row)

** INPUTS:
**		@RawValue: The raw string input to be cleaned.
**		@replacewith (optional): The replacement character to use for non-printable characters. (default is empty string)

** EXAMPLES:
**	-- Example 1: Clean a string input with non-printable characters.
**		SELECT dbo.udf_CleanString('hello, world' + CHAR(13) + CHAR(10) + 'how are you?', '') AS CleanedString;
**		-- Expected output: 'hello, worldhow are you?'
**
** -- Example 2: Clean a string input with extra spaces.
**		SELECT dbo.udf_CleanString(' hello, w  orld! ') AS CleanedString;
**		-- Expected output: 'hello, world!'
**
** KNOWN LIMITATIONS/BUGS:
** 	It can only be deployed in Synapse dedicated because serverless does not support scalar functions (only inline tvf)
**------------------------------------------------------
** FUTURE ENHANCEMENTS:
**		Error trapping for cases like attempts to pass a special character in as the replacement character

** CHANGE HISTORY:
-------------|------------------|------------------
	Date		Author				Change
------------|-------------------|------------------
**	20230301	J Pritchard			Initial Dev

**********************************************/




if exists (
	select * from sys.objects as o
		join sys.schemas as s on o.schema_id=s.schema_id
	where o.type='FN' and o.name like 'udf_CleanString' and s.name like 'dbo')
BEGIN
DROP FUNCTION dbo.udf_Cleanstring
END
GO


CREATE FUNCTION [dbo].[udf_CleanString](@RawValue varchar(4000),@replacewith varchar(10)='')
	RETURNS VARCHAR(4000)

	AS
	BEGIN

		DECLARE @S AS VARCHAR(MAX)=@RawValue
		DECLARE @i AS INT=0


		WHILE @i<=255
			BEGIN
				IF NOT @i BETWEEN 32 AND 126  --32-126 are the standard ' a-zA-Z0-9!;' etc characters
					BEGIN
						SET @S=REPLACE(@S,CHAR(@i),@replacewith)
					END

				SET @i=@i+1

			END

		SET  @S=trim(replace(replace(replace(@S,' ','†‡'),'‡†',''),'†‡',' ')) --handle multiple spaces in a row and leading/trailing

	RETURN @S

END