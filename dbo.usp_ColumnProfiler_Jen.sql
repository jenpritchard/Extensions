USE [fptadmin]
GO

/****** Object:  StoredProcedure [dbo].[usp_ColumnProfiler]    Script Date: 12/22/2021 11:30:53 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*=========================================================================================================
--	Author:			grady.bolte1@t-mobile.com
--	Create date:	--
--	Description:	This procedure is used to profile a column within the given table to get a better
					understanding of data contained within.	It is meant to be data type-agnostic in the 
					event that a column	is mis-typed.

					CHECKSUM NOTE:
					This is an explanation of the following clause in the WHERE statement
					of the dynamic SQL block:

						AND 0.1 >= CAST(CHECKSUM(NEWID(), t.' + @QuoteColumnName + ') & 0x7fffffff AS float)
							/ CAST (0x7fffffff AS int)

					This is put in place to generate a random subset of data stored within a table.  This
					significantly improves performance for this procedure as well as sp_TableProfiler
					especially on large tables.  The "0.1" in this example is where you set the percentage
					table that you want returned in the subset.  In this case, we are getting roughly 10% 
					of the whole.  The CHECKSUM logic is generating a random number between 0 and 1 which is
					then filtered by the percentage set in the beginning of the clause.
					
					See following article for more detailed information on how this works:
					https://www.mssqltips.com/sqlservertip/3157/different-ways-to-get-random-data-for-sql-server-data-sampling/


--	Usage:			EXEC usp_ColumnProfiler
						@ColumnName		= 'RequisitionID',
						@DatabaseName	= 'fptadmin',
						@SchemaName		= 'SRC_ARIBA',
						@TableName		= 't_PurchaseRequisition'
--	Changes:		
					Runtime pre checksum:	00:43
					Runtime post checksum:	00:06
==========================================================================================================*/

CREATE  OR ALTER   PROCEDURE [dbo].[usp_ColumnProfiler_Jen] (
	@ColumnName		varchar(50),
	@DatabaseName	varchar(255),
	@SchemaName		varchar(255),
	@TableName		varchar(255)
)

AS

SET NOCOUNT ON;
	
DECLARE @SQL nvarchar(MAX)
DECLARE @ThreePartTableName nvarchar(MAX) = @DatabaseName + '.' + @SchemaName + '.' + @TableName
DECLARE @RowCount int
DECLARE @WhereFilter nvarchar(MAX) = N''
DECLARE @QuoteColumnName varchar(52) = QUOTENAME(@ColumnName)

DROP TABLE IF EXISTS #DistinctValues

CREATE TABLE #DistinctValues (
	[ColumnValue]						nvarchar(4000),
	[sys_DATA_TYPE]						nvarchar(128),
	[sys_CHARACHTER_MAXIMUM_LENGTH]		int,
	[sys_IS_NULLABLE]					varchar(3),
	[sys_ORDINAL_POSITION]				int
)

SET @SQL = N'
	SELECT
		@out_RowCount = COUNT(1)
	FROM ' + @ThreePartTableName + '
'

EXEC sp_executesql @SQL, N'
	@out_RowCount int OUTPUT', 
	@RowCount OUTPUT;

-- See above "CHECKSUM" note for an explaination of what this clause does and why it is neccessary
SELECT
	@WhereFilter = 	CASE
			WHEN @RowCount > 200000000
			THEN N'AND 0.001 >= CAST(CHECKSUM(NEWID(), t.' + @QuoteColumnName + ') & 0x7fffffff AS float) / CAST (0x7fffffff AS int)'
			WHEN @RowCount > 20000000
			THEN N'AND 0.01 >= CAST(CHECKSUM(NEWID(), t.' + @QuoteColumnName + ') & 0x7fffffff AS float) / CAST (0x7fffffff AS int)'
			WHEN @RowCount > 2000000
			THEN N'AND 0.1 >= CAST(CHECKSUM(NEWID(), t.' + @QuoteColumnName + ') & 0x7fffffff AS float) / CAST (0x7fffffff AS int)'
			ELSE N''
		END

SET @SQL = N'
	INSERT INTO #DistinctValues
	SELECT
		CAST(t.' + @QuoteColumnName + ' AS NVARCHAR(4000)),
		isc.[DATA_TYPE], 
		isc.[CHARACTER_MAXIMUM_LENGTH],
		isc.[IS_NULLABLE],
		isc.[ORDINAL_POSITION]
	FROM ' + @ThreePartTableName + ' t
		CROSS JOIN ' + @DatabaseName + '.INFORMATION_SCHEMA.COLUMNS isc 
	WHERE isc.COLUMN_NAME = ''' + @ColumnName + '''
		AND isc.TABLE_CATALOG = ''' + @DatabaseName + '''
		AND isc.TABLE_SCHEMA = ''' + @SchemaName + '''
		AND isc.TABLE_NAME = ''' + @TableName + '''
		' + @WhereFilter + '
'

EXEC (@SQL);

WITH Lengths_CTE AS (
	SELECT 
		COUNT(DISTINCT [ColumnValue]) AS [DistinctCount],
		COUNT(1) AS [TotalCount],
		MAX(
			CASE
				WHEN [ColumnValue] is null
				THEN 0
				ELSE LEN([ColumnValue])
			END
		) AS [MaxLen],
		MIN(
			CASE
				WHEN [ColumnValue] is null
				THEN 0
				ELSE LEN([ColumnValue])
			END
		) AS [MinLen],
		MAX(
			CASE
				WHEN [ColumnValue] is null
				THEN 0
				ELSE CHARINDEX('.',REVERSE([ColumnValue])) -PATINDEX('%[^0]%',REVERSE([ColumnValue])) END
		) AS MaxDecimalPrecision
	FROM #DistinctValues
)

SELECT
	@QuoteColumnName AS [ColumnName],
	dv.[sys_DATA_TYPE],
	dv.[sys_CHARACHTER_MAXIMUM_LENGTH],
	dv.[sys_IS_NULLABLE],	
	dv.[sys_ORDINAL_POSITION],
	MAX(COALESCE(dv.[ColumnValue], '')) AS [MaxValue],
	MIN(COALESCE(dv.[ColumnValue], '')) AS [MinValue],
	l.[DistinctCount],
	l.[TotalCount],
	l.[MaxLen],
	l.[MinLen],
	CASE
		WHEN NOT EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE TRY_CAST([ColumnValue] AS decimal(19,4)) IS NULL 
				AND [ColumnValue] IS NOT NULL
		) AND EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE [ColumnValue] like '%.%'
		)
		THEN l.MaxDecimalPrecision
		ELSE 0
	END	AS [DecimalPrecisionRequired],
	CASE
		WHEN l.[MaxLen] = l.[MinLen]
		THEN l.[MaxLen]
		WHEN CAST(l.[DistinctCount] AS decimal(16,6)) / CAST(l.[TotalCount] AS decimal(16,6)) 
			< 0.001
		THEN l.[MaxLen]
		ELSE ((l.[MaxLen] + 50) / 50) * 50
	END AS [SuggestedLen],
	CASE
		WHEN EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE [ColumnValue] IS NULL
		)
		THEN 1
		ELSE 0
	END AS [HasNULLs],
	CASE
		WHEN EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE LEN([ColumnValue]) = 0
		)
		THEN 1
		ELSE 0
	END AS [HasBlanks],
	CASE
		WHEN NOT EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE LEN(COALESCE([ColumnValue],'')) > 0
		)
		THEN 1
		ELSE 0
	END AS [OnlyHasNULLsOrBlanks],
	CASE
		WHEN NOT EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE TRY_CAST([ColumnValue] AS decimal(19,4)) IS NULL 
				AND [ColumnValue] IS NOT NULL
		) AND EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE [ColumnValue] like '%.%'
		)
		THEN 1
		ELSE 0
	END	AS [CanBeDecimal],
	CASE
		WHEN EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE [ColumnValue] NOT LIKE '%[^0-9]%'
		) AND NOT EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE [ColumnValue] LIKE '%[^0-9]%'
				AND [ColumnValue] IS NOT NULL
		) 
		THEN 1
		ELSE 0
	END AS [CanBeInt],
	CASE
		WHEN NOT EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE TRY_CAST([ColumnValue] AS int) IS NULL 
				AND [ColumnValue] IS NOT NULL
		) 
		AND l.MaxDecimalPrecision=0
		THEN 1
		ELSE 0
	END AS [CanBeCastAsInt],
	CASE
		WHEN NOT EXISTS(
			SELECT 1
			FROM #DistinctValues
			WHERE COALESCE([ColumnValue], '0') NOT IN ('0', '1')
		)
		THEN 1
		ELSE 0
	END AS [CanBeBit],
	CASE
		WHEN NOT EXISTS(
			SELECT 1
			FROM #DistinctValues
			WHERE COALESCE([ColumnValue], 'N') NOT IN ('Y', 'N')
		)
		THEN 1
		ELSE 0
	END AS [BooleanYN],
	CASE
		WHEN NOT EXISTS(
			SELECT 1
			FROM #DistinctValues
			WHERE COALESCE([ColumnValue], 'No') NOT IN ('Yes', 'No')
		)
		THEN 1
		ELSE 0
	END AS [BooleanYesNo],
	CASE
		WHEN NOT EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE TRY_CAST([ColumnValue] AS date) IS NULL 
				AND [ColumnValue] IS NOT NULL
		)
		THEN 1
		ELSE 0
	END AS [CanBeDate],
	CASE
		WHEN EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE TRY_CAST([ColumnValue] AS time) > '00:00:00.000'
				AND [ColumnValue] IS NOT NULL
		)
		THEN 1
		ELSE 0
	END AS [HasTime],
	CASE
		WHEN EXISTS (
			SELECT 1
			FROM #DistinctValues
			WHERE TRY_CAST([ColumnValue] AS varchar(4000)) IS NULL 
				AND [ColumnValue] IS NOT NULL
		)
		THEN 1
		ELSE 0
	END AS [MustBeNvarchar]
FROM #DistinctValues dv
CROSS JOIN Lengths_CTE l
GROUP BY 
	dv.[sys_DATA_TYPE],
	dv.[sys_CHARACHTER_MAXIMUM_LENGTH],
	dv.[sys_IS_NULLABLE],	
	dv.[sys_ORDINAL_POSITION],
	l.[DistinctCount],
	l.[TotalCount],
	l.[MaxLen],
	l.[MinLen],
	l.MaxDecimalPrecision

GO


