USE [WorkDB]
GO

-- hacked from https://sqlstudies.com/2015/03/25/clean-out-all-bad-characters-from-a-string/
--		checkout comment by Brian Miller with more performant CLR function, but I don't think CLR is suppoerted in Synapse

-- trying to avoid RBAR: https://sqlstudies.com/2016/08/17/rbar-vs-batch/

-- Create a table with a bunch of rows and one column with string data.
-- The FROM is irrelevant, just building a table of strings the lazy way

-- DROP TABLE IF EXISTS [BadStringList]
SELECT CAST(t.name + ' ' + c.name AS NVARCHAR(MAX)) AS [StringToFix]
    INTO [BadStringList]
FROM sys.tables AS t
CROSS JOIN sys.all_columns AS c;

ALTER TABLE BadStringList
	ADD [Id] INT NOT NULL IDENTITY (1,1) CONSTRAINT pk_BadStringList PRIMARY KEY;

-- SELECT TOP 1000 * FROM [BadStringList] ORDER BY NEWID()
 
-- Put in one random (probably bad) character into about 2 percent of the rows.  Then do it 75 times
WITH CTE AS (
	SELECT
		TOP (2) percent [StringToFix]
		,STUFF([StringToFix]
			,CAST(rand((len([StringToFix]) * datepart(ms,getdate()))^2) * len([StringToFix]) AS Int) + 1, 1
			,NCHAR(CAST(rand((len([StringToFix]) * datepart(ms,getdate()))^2) * 65025 AS Int)))
		AS [Stuffed]
	FROM [BadStringList]
	ORDER BY NEWID()
)
UPDATE CTE
SET [StringToFix] = [Stuffed]
GO 75

DROP TABLE IF EXISTS #BadStringList_Test;
SELECT * INTO #BadStringList_Test -- 132748
FROM [BadStringList];


--- cleanup code
DECLARE @Pattern VARCHAR(50) = '%[^a-zA-Z0-9_''{}"() *&%$#@!?/\;:,.<>]%';
 
WITH [FixBadChars] AS (
	SELECT
		[StringToFix]
		,[StringToFix] AS [FixedString]
		,1 AS [MyCounter]
		,[Id]
	FROM #BadStringList_Test

	UNION ALL

	SELECT
		[StringToFix]
		,Stuff([FixedString], PatIndex(@Pattern, [FixedString] COLLATE Latin1_General_BIN2), 1, '') AS [FixedString] -- The COLLATE is necessary because otherwise some unicode characters get missed by the PATINDEX command. 
		,[MyCounter] + 1
		,[Id]
	FROM [FixBadChars]
	WHERE [FixedString] COLLATE Latin1_General_BIN2 LIKE @Pattern
)

SELECT [StringToFix], [FixedString], [MyCounter], [Id]
FROM [FixBadChars]
WHERE [MyCounter] = (
	SELECT MAX(MyCounter) 
	FROM [FixBadChars] AS [Fixed]
	WHERE [Fixed].Id = [FixBadChars].Id
)
--AND [MyCounter] > 1
OPTION (MAXRECURSION 1000); -- default MAXRECURSION of 100 is probably sufficient in this case, but . . .