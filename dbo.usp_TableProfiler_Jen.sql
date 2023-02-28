USE [fptadmin]
GO

/****** Object:  StoredProcedure [dbo].[usp_TableProfiler]    Script Date: 12/22/2021 1:14:21 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*=========================================================================================================
--	Author:			grady.bolte1@t-mobile.com
--	Create date:	--
--	Description:	This procedure is used to profile a table to get a better understanding of the data 
					contained within.  It does so by executing the [dbo].[usp_ColumnProfiler] procedure for
					each column in the given table.
--	Usage:			EXEC usp_TableProfiler
						@DatabaseName	= 'intake',
						@SchemaName		= 'SRC_ARIBA',
						@TableName		= 't_PurchaseRequisition',
						@BuildView		= 0
--	Changes:		
==========================================================================================================*/

CREATE    OR ALTER PROCEDURE [dbo].[usp_TableProfiler_Jen](
	@DatabaseName	varchar(255),
	@SchemaName		varchar(255),
	@TableName 		varchar(255),
	@BuildView		int
)
AS

DECLARE @SQL			nvarchar(max)
DECLARE @ViewCode		nvarchar(max)
DECLARE @PartitionKey	nvarchar(255)

DROP TABLE IF EXISTS #ColumnProfiles; 

CREATE TABLE #ColumnProfiles (
	ColumnName						varchar(255),
	sys_DATA_TYPE					varchar(50),
	sys_CHARACTER_MAXIMUM_LENGTH	int,
	sys_IS_NULLABLE					varchar(3),
	sys_ORDINAL_POSITION			int,
	MaxValue						varchar(max),
	MinValue						varchar(max),
	DistinctCount					int,
	TotalCount						int,
	MaxLen							int,
	MinLen							int,
	DecimalPrecisionRequired		int,
	SuggestedLen					int,
	HasNULLs						int,
	HasBlanks						int,
	OnlyHasNULLsOrBlanks			int,
	CanBeDecimal					int,
	CanBeInt						int,
	CanBeCastAsInt					int,
	CanBeBit						int,
	BooleanYN						int,
	BooleanYesNo					int,
	CanBeDate						int,
	HasTime							int,
	MustBeNvarchar					int
)

SET @SQL = N'
	DECLARE @ColumnCursor	varchar(255)

	DECLARE column_cursor CURSOR FOR 
	SELECT COLUMN_NAME
	FROM ' + @DatabaseName + '.INFORMATION_SCHEMA.COLUMNS isc
	WHERE 1=1
		AND isc.TABLE_CATALOG = ''' + @DatabaseName + '''
		AND isc.TABLE_SCHEMA = ''' + @SchemaName + '''
		AND isc.TABLE_NAME = ''' + @TableName  + '''
	ORDER BY ORDINAL_POSITION 


	OPEN column_cursor  
	FETCH NEXT FROM column_cursor INTO @ColumnCursor  

	WHILE @@FETCH_STATUS = 0  
	BEGIN 
		PRINT ''Evaluating column: '' + @ColumnCursor
		INSERT INTO #ColumnProfiles
		EXEC [dbo].[usp_ColumnProfiler_Jen]
			@ColumnName	= @ColumnCursor,
			@DatabaseName = ''' + @DatabaseName + ''',
			@SchemaName	= ''' + @SchemaName + ''',
			@TableName = ''' + @TableName  + '''

		FETCH NEXT FROM column_cursor INTO @ColumnCursor
	END 

	CLOSE column_cursor  
	DEALLOCATE column_cursor 
'

EXEC (@SQL)

SELECT @PartitionKey = FIRST_VALUE(ColumnName) OVER (ORDER BY sys_ORDINAL_POSITION)
FROM #ColumnProfiles

IF @BuildView = 1
	BEGIN
		SET @ViewCode = N'
			WITH CTE AS (
				SELECT '

		SELECT @ViewCode = @ViewCode +
			CASE OnlyHasNULLsOrBlanks
				WHEN 1
				THEN ColumnName+ ','
				ELSE CASE
					WHEN CanBeInt = 1 AND CanBeCastAsInt = 1 AND CanBeBit <> 1
					THEN N'CAST([dbo].[udf_CleanNULLs](' + ColumnName + ')	AS int)		AS ' + ColumnName + ','
					WHEN CanBeInt = 1 AND CanBeCastAsInt = 0 AND CanBeBit <> 1
					THEN N'CAST([dbo].[udf_CleanNULLs](' + ColumnName + ')	AS bigint)		AS ' + ColumnName + ','
					WHEN CanBeBit = 1
					THEN N'CAST([dbo].[udf_CleanNULLs](' + ColumnName + ')	AS bit)		AS ' + ColumnName + ','
					WHEN BooleanYN = 1
					THEN N'CAST(
							CASE [dbo].[udf_CleanNULLs](' + ColumnName + ')
							WHEN ''Y''
							THEN 1
							WHEN ''N''
							THEN 0
							ELSE NULL
						END		AS bit)		AS ' + ColumnName + ','
					WHEN BooleanYesNo = 1
					THEN N'CAST(
							CASE [dbo].[udf_CleanNULLs](' + ColumnName + ')
							WHEN ''Yes''
							THEN 1
							WHEN ''No''
							THEN 0
							ELSE NULL
						END		AS bit)		AS ' + ColumnName + ','
					WHEN CanBeDecimal = 1
					THEN N'CAST([dbo].[udf_CleanNULLs](' + ColumnName + ')	AS decimal(25,'+CAST(DecimalPrecisionRequired AS VARCHAR(20))+'))		AS ' + ColumnName + ','
					WHEN CanBeDate = 1 AND HasTime = 0
					THEN N'CAST([dbo].[udf_CleanNULLs](' + ColumnName + ')	AS date)		AS ' + ColumnName + ','
					WHEN CanBeDate = 1 AND HasTime = 1
					THEN N'CAST([dbo].[udf_CleanNULLs](' + ColumnName + ')	AS datetime)		AS ' + ColumnName + ','
					WHEN MustBeNvarchar = 1
					THEN N'CAST([dbo].[udf_CleanNULLs](' + ColumnName + ')	AS nvarchar(' + CAST(((MaxLen + 50) / 50) * 50 AS nvarchar(10)) + '))		AS ' + ColumnName + ','
					ELSE N'CAST([dbo].[udf_CleanNULLs](' + ColumnName + ')	AS varchar(' + CAST(((MaxLen + 50) / 50) * 50 AS nvarchar(10)) + '))		AS ' + ColumnName + ','
				END
			END
		FROM #ColumnProfiles

		SET @ViewCode = @ViewCode + N'
			FROM ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '
		)

		SELECT *
		FROM CTE
		'

		SELECT @ViewCode
	END
ELSE
	BEGIN
		SELECT *
		FROM #ColumnProfiles
	END

GO


