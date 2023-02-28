USE FPTAdmin
GO

/***************************************************************************************************
Procedure:          dbo.usp_ConstraintNullCheck
Create Date:        2022-1-11
Author:             Jen Pritchard @Logic20/20
Description:        Given one source to target mapping from the metadata table, check that all required fields in the dest are populated in the source
Call by:            ETL 
Affected table(s):  Source to Target
Used By:            FPT DTL layer
Parameter(s):       Can either provide the foreign key from dbo.t_Metadata, or the individual table info
					@FK_Metadata
					@SourceDB
					@SourceSchema
					@SourceTable
					@DestDB
					@DestSchema
					@DestTable
Usage:              EXEC dbo.usp_Main
						@FK_Metadata=1,
					or
					EXEC dbo.usp_Main
                        @SourceDB='intake'
						@SourceSchema='mapping'
						@SourceTable='testsource'
						@DestDB='processed'
						@DestSchema='core'
						@DestTable='testdest
Returns:			Returns the number of columns that a problematic null was found, or -1 if failed (would also raise a level 16 error to trip SSIS)
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------

***************************************************************************************************/


CREATE OR ALTER PROCEDURE dbo.usp_ConstraintNullCheck (
	@FK_Metadata		BIGINT			=NULL,
	@SourceDB			VARCHAR(255)	=NULL,
	@SourceSchema		VARCHAR(255)	=NULL,
	@SourceTable		VARCHAR(255)	=NULL,
	@DestDB				VARCHAR(255)	=NULL,
	@DestSchema			VARCHAR(255)	=NULL,
	@DestTable			VARCHAR(255)	=NULL	
		)

AS





SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

BEGIN



DECLARE
	@sql NVARCHAR(MAX) = N''
	,@MetaDataCount AS TINYINT
	,@FullDestName VARCHAR(200)
	,@FullSourceName VARCHAR(200)
	,@maxcount	BIGINT
	,@i	BIGINT =0
	,@singlecolumn VARCHAR(200)
	,@ERROR AS VARCHAR(4000)
	,@IgnoreColumns VARCHAR(4000) = '(''DTLCreatedDT'',''DTLModifiedDT'',''DTL_ModifiedDT'',''DTL_ModifiedDT'')'
--	,@CreatedColumn VARCHAR(MAX) = '' --standard column names applicable to all tables
--	,@ModifiedColumn VARCHAR(MAX) = '' --standard column names applicable to all tables



	BEGIN TRY
	------------------------------------------------	
	--Light error handling for the parameters


IF @FK_Metadata IS NOT NULL
			BEGIN
				SELECT @SourceDB=meta.SourceDB,
						@SourceSchema=meta.SourceSchema,
						@SourceTable=meta.SourceTable,
						@DestDB=meta.DestDB,
						@DestSchema=meta.DestSchema,
						@DestTable=meta.DestTable
				FROM fptadmin.dbo.t_metadata AS meta WITH (NOLOCK)
				WHERE meta.ID=@FK_Metadata
			END
	ELSE
		BEGIN
			SELECT @MetaDataCount= COUNT(*)
			FROM fptadmin.dbo.t_metadata AS meta WITH (NOLOCK)
			WHERE @SourceDB=meta.SourceDB
						AND @SourceSchema=meta.SourceSchema
						AND @SourceTable=meta.SourceTable
						AND @DestDB=meta.DestDB
						AND @DestSchema=meta.DestSchema
						AND @DestTable=meta.DestTable
		END


		IF @FK_Metadata NOT IN (SELECT ID FROM dbo.t_Metadata WITH (NOLOCK) WHERE ID=@FK_Metadata)  
			BEGIN
				SET @ERROR ='Invalid parameter: @FK_Metadata does not exist in the table';
				THROW 51000, @ERROR, 1;
			END

		IF @MetaDataCount<>1 AND @SourceDB IS NOT NULL
			BEGIN
				SET @ERROR ='Invalid parameter: The given source/dest information is not found in dbo.t_metadata';
				THROW 51000, @ERROR, 1;
			END

		IF @FK_Metadata IS NULL AND @SourceDB IS NULL
				BEGIN
					SET @ERROR = 'ERROR: Must provide either the metadata foreign key or the source/dest information';
					THROW 51000, @ERROR, 1;
				END





--------------------------------------------------------	
	
SET @FullDestName=CONCAT(QUOTENAME(@DestDB),'.',QUOTENAME(@DestSchema),'.',QUOTENAME(@DestTable))
SET @FullSourceName=CONCAT(QUOTENAME(@SourceDB),'.',QUOTENAME(@SourceSchema),'.',QUOTENAME(@SourceTable))

DROP TABLE IF EXISTS #temp_columns
CREATE TABLE #temp_columns
	(columnname VARCHAR(200)
		,NullCount BIGINT)


SET @sql=CONCAT('INSERT INTO #temp_columns(columnname)
SELECT c.[name] AS columnname 
FROM ', @DestDB ,'.sys.columns AS c 
	JOIN ', @DestDB , '.sys.tables AS t
		ON t.object_id = c.object_id 
WHERE 
	t.object_id=OBJECT_ID(''', @FullDestName ,''')
	AND is_nullable =0
	AND is_identity =0
	AND c.[name] NOT IN ' , @IgnoreColumns)

EXEC sp_executesql @sql


--For each non-nullable column, check to see if all the incoming data is valid
SELECT @maxcount=COUNT(*) FROM #temp_columns

			WHILE @maxcount>@i
				BEGIN
						SELECT @singlecolumn=columnname
						FROM #temp_columns
						ORDER BY columnname
						OFFSET @i ROWS
						FETCH NEXT 1 ROWS ONLY



						SET @sql=CONCAT('UPDATE t1
							SET t1.NullCount = t2.c
							FROM #temp_columns AS t1
							JOIN (SELECT ''',@singlecolumn ,''' AS columnname,COUNT(1) AS c
									FROM ', @FullSourceName ,
									' WHERE ' , @singlecolumn ,' IS NULL) AS t2
								ON t1.columnname=t2.columnname')

			

						EXEC sp_executesql @sql

				
				
				SET @i=@i+1 
				END


SELECT COUNT(1)
FROM #temp_columns
WHERE NullCount <> 0


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