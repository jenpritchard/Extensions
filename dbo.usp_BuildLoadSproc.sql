USE FPTAdmin
GO

/***************************************************************************************************
Procedure:          dbo.usp_BuildLoadSproc
Create Date:        2022-04-06
Author:             Jen Pritchard @Logic20/20
Description:        For a given job id, will write the procedure to load it
Call by:            manual 
Affected table(s):  
Used By:            
Parameter(s):       @FK_ScheduleJob		BIGINT		--ID for the source to target for this job
					
Usage:              EXEC dbo.usp_BuildLoadSproc
						@FK_ScheduleJob =1
Returns:			Prints the code to run for the procedure creation
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------

***************************************************************************************************/


CREATE OR ALTER PROCEDURE dbo.usp_BuildLoadSproc (
			@FK_ScheduleJob		BIGINT
)

AS





SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

BEGIN


DECLARE @MainSQL NVARCHAR(MAX)

----------------
DECLARE @CreatedColumnPossibilities VARCHAR(1000) = '(''[DTLCreatedDT]'',''[DTL_CreatedDT]'')' --standard column names applicable to all tables
DECLARE @ModifiedColumnPossibilities VARCHAR(MAX) = '(''[DTLModifiedDT]'',''[DTL_ModifiedDT]'')' --standard column names applicable to all tables
DECLARE @TemporalColumns	VARCHAR(1000) ='(''[sysValidFrom]'',''[sysValidTo]'')' --in the case of a temporal table, ignore those system columns
DECLARE @CreatedColumn		VARCHAR(100)
DECLARE @ModifiedColumn		VARCHAR(100)
DECLARE @FK_Metadata		BIGINT
DECLARE @SourceLinkedServer VARCHAR(255)
DECLARE @SourceDB			VARCHAR(255)
DECLARE @SourceSchema		VARCHAR(255)
DECLARE @SourceTable		VARCHAR(255)
DECLARE @SourceSystem		VARCHAR(255)
DECLARE @SourceExtractType	VARCHAR(255)
DECLARE @FullSource			VARCHAR(4000)
DECLARE @DestDB				VARCHAR(255)
DECLARE @DestSchema			VARCHAR(255)
DECLARE @DestTable			VARCHAR(255)
DECLARE @FullTarget			VARCHAR(4000)
DECLARE @SQLPK				NVARCHAR(255)
DECLARE @TableId			BIGINT
DECLARE @SourcePK			VARCHAR(255)
DECLARE @DestPK				VARCHAR(255)
DECLARE @RunMode			VARCHAR(255)			 --Upsert,Reload,Append
DECLARE @FilterClause		VARCHAR(MAX)
DECLARE @isNeedsCleaning	TINYINT 
DECLARE @ScheduleName		VARCHAR(255)
DECLARE @isPKIdentity		INT
DECLARE @SourceCount		BIGINT
DECLARE @MetaDataCount		TINYINT=0
DECLARE @SQLColumns			NVARCHAR(MAX)
DECLARE @SQLMergeAllList	NVARCHAR(MAX)
DECLARE @SQLMergeUpdate		NVARCHAR(MAX)
DECLARE @SQLListExcludeIdentity NVARCHAR(MAX)
DECLARE @SQLCleaned			NVARCHAR(MAX)
DECLARE @SqlUpdateValueChangeCheck NVARCHAR(MAX)
DECLARE @SQLMergeInsert		NVARCHAR(MAX)
DECLARE @SQLStatement		NVARCHAR(MAX)
DECLARE @CurrentDT			DATETIME = CURRENT_TIMESTAMP

DECLARE @ERROR VARCHAR(4000)=''
DECLARE @SprocName NVARCHAR(MAX)






BEGIN TRY






--Get all information about the source and target based on the given schedulejob id	

		SELECT 
				@FK_Metadata= meta.ID
				,@SourceLinkedServer=meta.SourceLinkedServer
				,@SourceDB=meta.SourceDB
				,@SourceSchema=meta.SourceSchema
				,@SourceTable=meta.SourceTable
				,@SourcePK=meta.SourcePK
				,@DestDB=meta.DestDB
				,@DestSchema=meta.DestSchema
				,@DestTable=meta.DestTable
				,@DestPK=meta.DestPK
				,@FullSource=CONCAT(QUOTENAME(meta.SourceDB),'.',QUOTENAME(meta.SourceSchema),'.',QUOTENAME(meta.SourceTable))
				,@FullTarget=CONCAT(QUOTENAME(meta.DestDB),'.',QUOTENAME(meta.DestSchema),'.',QUOTENAME(meta.DestTable))
				,@RunMode=s.LoadType
				,@SourceExtractType=meta.SourceExtractType
				,@FilterClause=meta.FilterClause
				,@isNeedsCleaning=meta.NeedsNullClean
			FROM fptadmin.dbo.t_JobSchedules AS s --we only care about tables in this schedule	
				JOIN fptadmin.dbo.t_Metadata AS meta
					ON s.FK_Metadata=meta.ID
			WHERE s.ID=@FK_ScheduleJob


		

		IF @FK_Metadata IS NULL --The parameter isn't in the jobschedules table
		BEGIN
			SET @ERROR ='Invalid parameter: The given source/dest + job information is not valid';
			THROW 51000, @ERROR, 1;
		END


------------------------------------------------


SET @SprocName=CONCAT(@DestDB,'_',@DestSchema,'_',@DestTable,'_',@RunMode)

			--start logging
	SET @MainSQL=CONCAT('	



	CREATE OR ALTER PROCEDURE dbo.usp_Load_',@SprocName,' (
		@FK_ScheduleJob		BIGINT,
		@ETL_ID				BIGINT


		)

AS




SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

BEGIN

BEGIN TRY

DECLARE @DeletedCount BIGINT= 0
DECLARE @InsertedCount BIGINT =0
DECLARE @UpdatedCount BIGINT =0
DECLARE @LogId		BIGINT=-777
DECLARE @FK_Metadata		BIGINT


EXEC @LogId=dbo.usp_ETLLog @ParentID=@ETL_ID ,@FK_Metadata=@FK_Metadata
	
')








			--Grab a list of all columns in the destination table

			DROP TABLE IF EXISTS #temp_Columns;
			CREATE TABLE #temp_Columns(ColumnName VARCHAR(255)
						,Ord INT
						,is_identityexclude TINYINT
						,is_CreatedDT TINYINT
						,is_ModifiedDT TINYINT
						,is_PK	TINYINT);


			SET @SQlColumns=CONCAT('INSERT INTO #temp_Columns(ColumnName,Ord, is_identityexclude,is_CreatedDT,is_ModifiedDT,is_PK)
				SELECT QUOTENAME(col.name) AS columnname
					,column_id AS ord
					,CASE WHEN col.is_identity=1 OR col.is_computed=1 THEN 1 ELSE 0 END AS is_identityexclude 
					,CASE WHEN QUOTENAME(col.name) IN ' ,@CreatedColumnPossibilities ,' THEN 1 ELSE 0 END AS is_CreatedDT
					,CASE WHEN QUOTENAME(col.name) IN ' ,@ModifiedColumnPossibilities ,' THEN 1 ELSE 0 END AS is_ModifiedDT
					,CASE WHEN QUOTENAME(col.name)=''',@DestPK,''' THEN 1 ELSE 0 END AS is_PK
				FROM ' ,@DestDB ,'.sys.columns AS col WITH (NOLOCK)
				WHERE col.object_id = OBJECT_ID(''' ,@FullTarget ,''')
					AND QUOTENAME(col.name) NOT IN ',@TemporalColumns
				)
	

							
				EXEC sp_executesql @SQlColumns;



			--get a list of all columns minus any identity ones (since those won't be needed in any insert)
				SELECT @SQLListExcludeIdentity=STRING_AGG(TRY_CAST(ColumnName AS VARCHAR(MAX)) ,',') WITHIN GROUP (ORDER BY ColumnName ASC)
				FROM #TEMP_Columns 
				WHERE (is_identityexclude=0 AND is_CreatedDT=0 AND is_ModifiedDT=0)


				SELECT @SQLCleaned=STRING_AGG(TRY_CAST(
					CONCAT(' CASE TRY_CAST(',ColumnName,' AS VARCHAR(4000))
								WHEN '''' THEN NULL
								WHEN ''NULL'' THEN NULL
								ELSE ',ColumnName
								,' END AS ',ColumnName
							
							)
				 AS VARCHAR(MAX)) ,',') WITHIN GROUP (ORDER BY ColumnName ASC)
				 FROM #TEMP_Columns 
				WHERE (is_identityexclude=0 AND is_CreatedDT=0 AND is_ModifiedDT=0)


				SELECT @SQLMergeAllList=STRING_AGG(TRY_CAST(ColumnName AS VARCHAR(MAX)) ,',') WITHIN GROUP (ORDER BY ColumnName ASC)
				FROM #TEMP_Columns --a list of all columns
				WHERE is_CreatedDT=0 AND is_ModifiedDT=0
	

				SELECT @SQLMergeUpdate=STRING_AGG(TRY_CAST(CONCAT(ColumnName, '= src.' ,ColumnName) AS VARCHAR(MAX)) ,',') WITHIN GROUP (ORDER BY ColumnName ASC)
				FROM #TEMP_Columns
				WHERE is_PK=0 AND is_identityexclude=0 AND is_CreatedDT=0 AND is_ModifiedDT=0 --target to source assignments except for primary key column and identity columns
	

				SELECT @SQLMergeInsert=STRING_AGG(TRY_CAST(CONCAT('src.',ColumnName )AS VARCHAR(MAX)),',') WITHIN GROUP (ORDER BY ColumnName ASC)
				FROM #TEMP_Columns
				WHERE is_identityexclude=0 AND is_CreatedDT=0 AND is_ModifiedDT=0

				SELECT @CreatedColumn=ColumnName
				FROM #temp_Columns
				WHERE is_CreatedDT=1

				SELECT @ModifiedColumn=ColumnName
				FROM #temp_Columns
				WHERE is_ModifiedDT=1

				--ended up being better than a hash across all columns, since the concat within it would be a mismatch of unkonwn data types
				SELECT @SqlUpdateValueChangeCheck=STRING_AGG(TRY_CAST(CONCAT(' ((tgt.',ColumnName, '<> src.' ,ColumnName,')
						OR (tgt.',ColumnName, ' IS NULL AND src.' ,ColumnName,' IS NOT NULL)
						OR (tgt.',ColumnName, ' IS NOT NULL AND src.' ,ColumnName,' IS NULL))'		
				
				) AS VARCHAR(MAX)) ,',')
				FROM #TEMP_Columns
				WHERE is_PK=0 AND is_identityexclude=0 AND is_CreatedDT=0 AND is_ModifiedDT=0
				SET @SqlUpdateValueChangeCheck=REPLACE(@SqlUpdateValueChangeCheck,',',' OR ')







						IF  @RunMode ='Upsert'

						BEGIN

	

						SET @MainSQL=CONCAT(@MainSQL,'	IF OBJECT_ID(''tempdb..#TEMP_RowCount'') IS NOT NULL
								DROP TABLE #TEMP_RowCount
							;

							CREATE TABLE #TEMP_RowCount(
								ActionTaken VARCHAR(255)
							);')

					
							SET @SQLStatement= CONCAT
								('BEGIN TRANSACTION
								MERGE ' , @FullTarget , ' AS tgt
									USING (SELECT ',
									CASE WHEN COALESCE(@isNeedsCleaning,0)=1 THEN @SQLCleaned ELSE '*' END
									,'  FROM ' , @FullSource , ' WITH(NOLOCK) '
									,@FilterClause --optional where clause
									,' ) AS src ON ' ,
										'tgt.', @DestPK , ' = src.' ,@SourcePK ,
									' WHEN MATCHED AND ( ',@SqlUpdateValueChangeCheck,' )THEN UPDATE SET ' , @SQLMergeUpdate ,
									' , tgt.' , @ModifiedColumn ,' = ''' , TRY_CAST(@CurrentDT AS VARCHAR(100)) ,
									''' WHEN NOT MATCHED BY TARGET THEN 
										INSERT (' , @SQLListExcludeIdentity , ',' , @CreatedColumn, ',' , @ModifiedColumn , ') VALUES (',@SQLMergeInsert , 
										', ''' , TRY_CAST(@CurrentDT AS VARCHAR(100)) , ''', ''' , TRY_CAST(@CurrentDT AS VARCHAR(100)) ,''')')

								SET @SQLStatement= CONCAT(@SQLStatement,CASE WHEN @SourceExtractType='Full' THEN 
										' WHEN NOT MATCHED BY SOURCE
											THEN DELETE ' END) --don't delete records that aren't found if it's just a delta extract

								SET @SQLStatement=CONCAT(@SQLStatement,'	 OUTPUT $action AS ActionTaken
									 INTO #TEMP_RowCount;
									 COMMIT TRANSACTION
									 
									 SELECT 
								@DeletedCount=COALESCE(SUM(CASE WHEN ActionTaken=''Delete'' THEN 1 ELSE 0 END),0)
								,@InsertedCount=COALESCE(SUM(CASE WHEN ActionTaken=''Insert'' THEN 1 ELSE 0 END),0)
								,@UpdatedCount=COALESCE(SUM(CASE WHEN ActionTaken=''Update'' THEN 1 ELSE 0 END),0)
							FROM #TEMP_RowCount')
					
							
				
							


							






						END

						ELSE --  @RunMode IN ('Reload','Append') 

							BEGIN
							
								

									SET @SQLStatement ='BEGIN TRANSACTION
									'
										IF @RunMode ='Reload' --not applicable for append
											BEGIN
												SET @SQLStatement = CONCAT(@SQLStatement,
	'
	TRUNCATE TABLE ' , @FullTarget , '; 
		')
											END 

									SET @SQLStatement =CONCAT(@SQLStatement,'
	INSERT INTO ' , @FullTarget , ' (' ,
										@SQLListExcludeIdentity, ',' , @CreatedColumn , ',' , @ModifiedColumn, ') 
		SELECT ' ,
										CASE WHEN COALESCE(@isNeedsCleaning,0)=1 THEN @SQLCleaned ELSE @SQLListExcludeIdentity END
										, ',''', TRY_CAST(@CurrentDT AS VARCHAR(100)), ''',''', TRY_CAST(@CurrentDT AS VARCHAR(100)) ,
										''' FROM ' , @FullSource ,'  WITH(NOLOCK) ',
										@FilterClause, --optional where clause
										' ; 
										
		SELECT @InsertedCount= ROWCOUNT_BIG()
COMMIT TRANSACTION
			SET @InsertedCount=COALESCE(@InsertedCount,0)
			SET @UpdatedCount=0
			SET @DeletedCount=0' )

								
										
	END 

					




SET @mainSQL=CONCAT(@mainSQL,@SQLStatement,'
		
		
EXEC dbo.usp_UpdateMetadataSize @FK_Metadata
EXEC dbo.usp_ETLLog @RowID = @LogId,@RowsInserted = @InsertedCount, @RowsUpdated=@UpdatedCount,@RowsDeleted	=@DeletedCount,@RunStatus = ''Success''
		
		
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
            DECLARE @ErrorSeverity INT;
            DECLARE @ErrorState INT;
 
            SELECT
                @ErrorMessage = ERROR_MESSAGE(),
                @ErrorSeverity = ERROR_SEVERITY(),
                @ErrorState = ERROR_STATE();
 
			EXEC dbo.usp_ETLLog @RowID = @LogId,@RowsInserted = @InsertedCount, @RowsUpdated=@UpdatedCount,@RowsDeleted	=@DeletedCount, @ErrMessage=@ErrorMessage,@RunStatus = ''Failed''


            RAISERROR (@ErrorMessage,
                       11, --or @ErrorSeverity, if not called within an SSIS package
                       @ErrorState
                       );
         
        RETURN -1

	END CATCH

END

'
)

--PRINT @mainSQL
EXEC sp_executesql @mainSQL
END TRY
-----------------------------------------
BEGIN CATCH
 
        --First, rollback the transactions
            IF @@TRANCOUNT>0
                BEGIN
                    ROLLBACK TRANSACTION
                END
 
        --Now prep and return the error message
 
            DECLARE @ErrorMessage NVARCHAR(4000);
            DECLARE @ErrorSeverity INT;
            DECLARE @ErrorState INT;
 
            SELECT
                @ErrorMessage = ERROR_MESSAGE(),
                @ErrorSeverity = ERROR_SEVERITY(),
                @ErrorState = ERROR_STATE();
 
	--		EXEC dbo.usp_ETLLog @RowID = @LogId,@RowsInserted = @InsertedCount, @RowsUpdated=@UpdatedCount,@RowsDeleted	=@DeletedCount, @ErrMessage=@ErrorMessage,@RunStatus = 'Failed'


            RAISERROR (@ErrorMessage,
                       11, --or @ErrorSeverity, if not called within an SSIS package
                       @ErrorState
                       );
         
        RETURN -1

	END CATCH

END

GO

	