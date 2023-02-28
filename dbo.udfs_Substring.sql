IF NOT EXISTS (SELECT *
               FROM   sys.objects
               WHERE  object_id = OBJECT_ID(N'[dbo].[udfs_Substring]')
                      AND type IN ( N'FN', N'IF', N'TF', N'FS', N'FT' ))
       EXEC('CREATE FUNCTION dbo.udfs_Substring() RETURNS INT AS BEGIN RETURN 0 END')
  GO

/******************TODO
Might as well remove support for pattern matching, since there's no good way to find the length of the startpat
Or, create a loop that starts at the startpos, then increments from startpos+1 until endpos. It would take little substrings at a time, check if it would match the pattern
	(taking out all %) to see if an exact match to that would work.
	*Caveat: that wouldn't work if there are patterns like [a-z][a-z] because one char at a time wouldn't see the second [a-z].
	* Maybe add logic that can detect the min length of a string then use that as the number of char to iterate through? Eh, I don't think that'd work either :/
**********************/
ALTER FUNCTION dbo.udfs_Substring --put in it's own "overload" schema
(			@fullString NVARCHAR(MAX) 
			,@startString NVARCHAR(MAX) 
			,@endString NVARCHAR(MAX) 
			,@ispat BIT=0 --defaults to being able to enter it like a charindex, but ispat=1 allows format like patindex
			,@includestart BIT=0 --will include the start string in the result (useful if this is trying to parse key/value)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN

	DECLARE @truncatedstring AS NVARCHAR(MAX)
			,@startpat NVARCHAR(MAX)
			,@startpos INT
			,@beginsub INT
			,@endpat NVARCHAR(MAX)
			,@endpos INT
			,@finishsub INT
			




IF COALESCE(@fullString,'')=''  RETURN NULL;





    -- If starting thing to find is null or blank, start at the beginning
	--Otherwise, use patindex to support regular expressions as a valid input



		SET @startpat= CASE WHEN @ispat=0 THEN CONCAT('%',@startString,'%') ELSE @startString END --convert it into a pattern match
		SET @startpos =PATINDEX(@startpat, @fullString) --location of where the string pattern first appears
		SET @beginsub= @startpos+LEN(@startString)




IF COALESCE(@startpos,0) =0 OR @beginsub >= LEN(@fullString) 
	RETURN NULL --starting string not found, or the start string was at the end of the full string


-----------------------


--if the starting substring was found, keep going by creating an abbreviated form of the string

IF @includestart=1 
	SET @truncatedstring= SUBSTRING(@fullString,@startpos,LEN(@fullString))
ELSE
	SET @truncatedstring= SUBSTRING(@fullString,@beginsub,LEN(@fullString))






IF COALESCE(@endstring,'')='' SET @endpos=LEN(@truncatedstring)  
ELSE
	BEGIN
		SET @endpat= CASE WHEN @ispat=0 THEN CONCAT('%',@endString,'%') ELSE @endString END --convert it into a pattern match
		SET @endpos= PATINDEX(@endpat,@truncatedstring)-1
	END


RETURN CASE WHEN @endpos<=0 THEN NULL
		ELSE SUBSTRING(@truncatedstring, 1, @endpos)
		END ;



END


/* Did it this way with patindex so that there's an option to use regex. Otherwise, below is a good option:

  RETURN SUBSTRING(@col, CHARINDEX(@start, @col) + LEN(@start), 
         ISNULL(NULLIF(CHARINDEX(@end, STUFF(@col, 1, CHARINDEX(@start, @col)-1, '')),0),
         LEN(STUFF(@col, 1,CHARINDEX(@start, @col)-1, ''))+1) - LEN(@start)-1);
		 */