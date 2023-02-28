USE fptadmin
GO

----/***************************************************************************************************
----Procedure:          dbo.usp_ConstraintUniqueCheck
----Create Date:        2022-01-10
----Author:             Jen Pritchard @Logic20/20
----Description:        Checks the uniqueness of a given table's columns. Columns checked are determined
----					by its SourcePKs in the metadata table
----Call by:            ETL prior to all load steps
----Affected table(s):  --
----Used By:            FPT DTL layer
----Parameter(s):       @SourceDB			VARCHAR(255)	--Database name of the table
--						@SourceSchema		VARCHAR(255)	--Schema name of the table
--						@SourceTable		VARCHAR(255)	--Name of the table
----Usage:              EXEC dbo.usp_ConstraintUniqueCheck 'intake','SRC_MGB','v_DTL_CSE_TOTAL_POR_REPORT'
----Returns:			Returns a count of columns that weren't unique. 0 would indicate that all PK columns are unique

----****************************************************************************************************
----SUMMARY OF CHANGES
----Date(yyyy-mm-dd)    Author              Comments
----------------------- ------------------- ------------------------------------------------------------

----***************************************************************************************************/


CREATE OR ALTER PROCEDURE dbo.usp_ConstraintUniqueCheck 
	@SourceDB			VARCHAR(255),
	@SourceSchema		VARCHAR(255),
	@SourceTable		VARCHAR(255)	
AS	

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

 DECLARE @sql NVARCHAR(MAX) = N''
			,@parameters AS NVARCHAR(MAX)
			,@MetaDataCount AS INT
			,@RowCount AS BIGINT
			,@ERROR AS VARCHAR(4000)
			,@FullSourceName AS VARCHAR(200)
			,@maxcount	BIGINT
			,@i	BIGINT =0
			,@UniqueColumnName VARCHAR(200)
			,@UniqueCount BIGINT
			
BEGIN
	BEGIN TRY


			SET @FullSourceName=CONCAT(QUOTENAME(@SourceDB),'.',QUOTENAME(@SourceSchema),'.',QUOTENAME(@SourceTable))


--Error checking
			SELECT @MetaDataCount= COUNT(*)
			FROM fptadmin.dbo.t_metadata AS meta WITH (NOLOCK)
			WHERE @SourceDB=meta.SourceDB
						AND @SourceSchema=meta.SourceSchema
						AND @SourceTable=meta.SourceTable

			IF @MetaDataCount<1 
				BEGIN
					SET @ERROR ='Invalid parameter: The given source/dest information is not found in dbo.t_metadata';
					THROW 51000, @ERROR, 1;
				END
------------------------------------------------------------------------
--Table containing all columns to be checked for uniqueness
DROP TABLE IF EXISTS #temp_column
SELECT DISTINCT SourcePK
	,0 AS UniqueCount
INTO #temp_column
FROM fptadmin.dbo.t_Metadata AS meta
WHERE @SourceDB=meta.SourceDB
		AND @SourceSchema=meta.SourceSchema
		AND @SourceTable=meta.SourceTable


--How many rows are in the table total?
SET @sql=CONCAT('SELECT @RowCount=COUNT(1)
		FROM ' , @FullSourceName)
	SET @parameters='@RowCount BIGINT OUT'
	EXEC sp_executesql @sql, @parameters, @RowCount OUTPUT


SELECT @maxcount=COUNT(*) FROM #temp_column

			WHILE @maxcount>@i
				BEGIN
							--Grab 1 column at a time
							SELECT DISTINCT
									@UniqueColumnName=SourcePK
							FROM #temp_column
							ORDER BY SourcePK
							OFFSET @i ROWS
							FETCH NEXT 1 ROWS ONLY

							--What's the unique count of that column?
							SET @sql=CONCAT('SELECT @UniqueCount= COUNT(DISTINCT ' , @UniqueColumnName ,')
									FROM ' , @FullSourceName)
							SET @parameters='@UniqueCount BIGINT OUT'
							EXEC sp_executesql @sql, @parameters, @UniqueCount OUTPUT

							--Reflect this count in the temp table
							SET @sql=CONCAT('UPDATE #temp_column
							SET UniqueCount= ', TRY_CAST(@UniqueCount AS NVARCHAR(20))
							,' WHERE SourcePK=''' , @UniqueColumnName , '''')
							EXEC sp_executesql @sql


							SET @i=@i+1 
				END

			SELECT 
				SUM(CASE WHEN UniqueCount <> @RowCount THEN 1 ELSE 0 END) AS flag
			FROM #temp_column



END TRY
-------------------------------------------
	BEGIN CATCH

			--First, rollback the transactions
				IF @@TRANCOUNT>0 
					BEGIN 
						ROLLBACK TRANSACTION 
					END

			--Now prep and return the error message

				DECLARE @ErrorMessage NVARCHAR(4000);

				SET @ErrorMessage = ERROR_MESSAGE();

				THROW 51000, @ErrorMessage, 1;
		
			RETURN -1

	END CATCH
END

GO	