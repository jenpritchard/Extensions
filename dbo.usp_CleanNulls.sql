DECLARE @DB AS VARCHAR(255)='intake'
, @Schema AS VARCHAR(255)='clean'
, @Table AS VARCHAR(255)='t_MGB_DTL_CSE_Final_Report'






DECLARE @SQLColumns AS NVARCHAR(MAX)
DECLARE @SQLStatement AS NVARCHAR(MAX)
DECLARE @MaxOrd AS INT
DECLARE @FullCleanName AS VARCHAR(4000)=CONCAT(QUOTENAME(@DB),'.',QUOTENAME(@Schema),'.',QUOTENAME(@Table))





DROP TABLE IF EXISTS #temp_Columns;
CREATE TABLE #temp_Columns(ColumnName VARCHAR(255) NULL
							,Ord INT NULL);

--Using a temp table because of needing the dynamic sql for the DB
			SET @SQlColumns=CONCAT('INSERT INTO #temp_Columns(ColumnName,Ord)
				SELECT col.column_name,Ordinal_Position
				FROM ' ,@DB ,'.INFORMATION_SCHEMA.COLUMNS AS col
				WHERE col.table_schema = ''',@Schema,'''
					AND col.Table_Name=''',@Table,'''
					AND col.Data_type IN (''varchar'',''nvarchar'',''char'')'
				) --only looks at these datatypes because things like int, date etc wouldn't show up with a string of ' ' or the word 'null' anyway
					--instead, they'd have a null or some sort of dummy value we can't predict
	
			
			EXEC sp_executesql @SQlColumns;
			SELECT @MaxOrd=MAX(Ord) FROM #temp_Columns




SET @SQLStatement=CONCAT('UPDATE ',@FullCleanName,' SET ')

SELECT @SQLStatement=CONCAT(@SQLStatement,
			ColumnName, ' =CASE WHEN TRIM(', ColumnName, ') IN (''NULL'','''') THEN NULL ELSE ',ColumnName, ' END '
			, CASE
				WHEN @MaxOrd = Ord THEN N''
				ELSE N','
				END)
FROM #temp_Columns
ORDER BY Ord --Adding this to make extra sure the last column we're looking at is the last one




select @SQLStatement
--EXEC sys.sp_executesql @SQLStatement



