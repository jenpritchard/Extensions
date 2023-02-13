
--DROP FUNCTION [dbo].[udfs_StringSplit]


CREATE FUNCTION [dbo].[udfs_StringSplit]  (
	@InputString NVARCHAR(MAX)
	, @Delimiter NVARCHAR(100)
	)

RETURNS TABLE --@Output TABLE (Value NVARCHAR(MAX) NOT NULL, Ordinal INT NULL)
AS
RETURN

(

	SELECT value,ordinal from STRING_SPLIT(REPLACE(@InputString,@Delimiter,NCHAR(9999)),NCHAR(9999),1)

)

