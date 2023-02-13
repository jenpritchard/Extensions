CREATE FUNCTION dbo.udfs_Substring --put in it's own "overload" schema
(  @fullString NVARCHAR(MAX) NOT NULL
			,@startString NVARCHAR(MAX) NOT NULL
			,@endString NVARCHAR(MAX) NOT NULL
			,@ispat BIT=0 --defaults to being able to enter it like a charindex, but ispat=1 allows format like patindex
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
			




IF COALESCE(@fullString,'')=''  
	OR COALESCE(@startString,'')=''
	OR COALESCE(@endString,'')=''
RETURN NULL;



    -- If starting thing to find is null or blank, start at the beginning
	--Otherwise, use patindex to support regular expressions as a valid input

SET @startpat= CASE WHEN @ispat=0 THEN CONCAT('%',@startString,'%') ELSE @startString END --convert it into a pattern match
SET @startpos =PATINDEX(@startpat, @fullString) --location of where the string pattern first appears
SET @beginsub= @startpos+LEN(@startString)



IF @startpos =0 RETURN NULL --starting string not found


-----------------------


--if the starting substring was found, keep going by creating an abbreviated form of the string

SET @truncatedstring= SUBSTRING(@fullString,@beginsub,LEN(@fullString))
SET @endpat= CASE WHEN @ispat=0 THEN CONCAT('%',@endString,'%') ELSE @endString END --convert it into a pattern match
SET @endpos=PATINDEX(@endpat,@truncatedstring)
SET @finishsub=@endpos+@beginsub



IF @endpos IS NULL RETURN NULL


 RETURN  SUBSTRING(@truncatedstring, 1, @endpos-1);

END


/* Did it this way with patindex so that there's an option to use regex. Otherwise, below is a good option:

  RETURN SUBSTRING(@col, CHARINDEX(@start, @col) + LEN(@start), 
         ISNULL(NULLIF(CHARINDEX(@end, STUFF(@col, 1, CHARINDEX(@start, @col)-1, '')),0),
         LEN(STUFF(@col, 1,CHARINDEX(@start, @col)-1, ''))+1) - LEN(@start)-1);
		 */