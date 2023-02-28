USE fptadmin;
GO

DECLARE @sql NVARCHAR(MAX)  
DECLARE @sql2 NVARCHAR(MAX)

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;





DROP TABLE IF EXISTS #temp_dblist

SELECT TRY_CAST(database_id AS VARCHAR(MAX)) AS [database_id_varchar],database_id AS database_id_int, [name] AS [database_name]
INTO #temp_dblist
FROM sys.databases WITH (NOLOCK)
WHERE 1 = 1
    AND [state] <> 6 /* ignore offline DBs */
    AND database_id > 4 /* ignore system DBs */
	AND name NOT LIKE 'SSISDB'


--list of all objects to script
DROP TABLE IF EXISTS #temp_allobjects 
CREATE TABLE #temp_allobjects(	ObjectDB VARCHAR(500)
				,ObjectSchema VARCHAR(500)
				,ObjectName VARCHAR(500)
				,ObjectID INT
				,type_desc VARCHAR(250)
				,definition VARCHAR(MAX)
				,Unscripted INT
				)
SET @sql='INSERT INTO #temp_allobjects(ObjectDB,ObjectSchema,ObjectName,ObjectID,type_desc,definition, Unscripted)'
SELECT @sql=CONCAT(@sql,STRING_AGG(CONCAT('SELECT ''', [database_name],''' AS SourceDB
					,OBJECT_SCHEMA_NAME(o.object_id,',[database_id_varchar],') AS SourceSchema
					,OBJECT_NAME(o.object_id,',[database_id_varchar],') AS sourceObject
					,o.object_id
					,type_desc
					,sm.definition
					,1
				FROM ',[database_name],'.sys.objects AS o WITH (NOLOCK)
					LEFT JOIN ',[database_name],'.sys.sql_modules sm WITH (NOLOCK)
						ON o.object_id = sm.object_id
				WHERE type_desc IN (''SQL_STORED_PROCEDURE''
						,''SQL_TABLE_VALUED_FUNCTION''
						,''SQL_SCALAR_FUNCTION''
						,''SQL_INLINE_TABLE_VALUED_FUNCTION''
						,''USER_TABLE''
						,''VIEW''
						)
					AND is_ms_shipped=0'),' UNION '))
	FROM #temp_dblist


EXEC sp_executesql @sql


-------------------------------------







--list of dependency info
DROP TABLE IF EXISTS #temp_dependencies
CREATE TABLE #temp_dependencies(	SourceDB VARCHAR(500)
								,SourceSchema VARCHAR(500)
								,sourceObject VARCHAR(500)
								,usedDB VARCHAR(500)
								,usedSchema VARCHAR(500)
								,usedTable VARCHAR(500)
								)

SET @sql2='INSERT INTO #temp_dependencies(SourceDB,SourceSchema,sourceObject,usedDB,usedSchema,usedTable)'

SELECT @sql=STRING_AGG(
						TRY_CAST(
							CONCAT(
								' SELECT  DISTINCT 
									o.ObjectDB
									,o.ObjectSchema
									,o.ObjectName
									,ISNULL(referenced_database_name, o.ObjectDB) AS usedDB
									,referenced_schema_name AS usedSchema
									,referenced_entity_name  AS usedTable
							FROM #temp_allobjects AS o
								LEFT JOIN ',d.[database_name],'.sys.sql_expression_dependencies as dep WITH (NOLOCK) 
									ON  o.ObjectID=dep.referencing_id
								LEFT JOIN #temp_allobjects AS ofilter
									ON ofilter.ObjectDB=ISNULL(dep.referenced_database_name, o.ObjectDB)
										AND ofilter.ObjectSchema=ISNULL(dep.referenced_schema_name, o.ObjectSchema)
										AND ofilter.ObjectName=ISNULL(dep.referenced_entity_name, o.ObjectName)
							WHERE o.ObjectDB=''',d.[database_name],'''
								AND NOT (dep.referencing_id IS NOT NULL AND ofilter.ObjectID IS NULL)'
									)
								AS VARCHAR(MAX))
									,' UNION ')
						WITHIN GROUP (ORDER BY d.[database_name] ASC)	
					
FROM 
#temp_dblist AS d

SET @sql=CONCAT(@sql2,@sql)

EXEC sp_executesql @sql






--This part is only needed because there's a MSFT "bug" with sys.sql_expression_dependencies that limits it to only working with views. 


SELECT @sql=STRING_AGG(TRY_CAST(
	CONCAT('MERGE INTO #temp_dependencies AS tgt
				USING (SELECT ObjectDB AS SourceDB, ObjectSchema AS SourceSchema,ObjectName AS SourceObject,''',a.ObjectDB,''' AS usedDB,''',a.ObjectSchema,''' AS usedSchema,''',a.ObjectName,''' AS usedTable 
			FROM #temp_allobjects
			WHERE definition LIKE ''%',a.ObjectName,'[^a-z0-9_]%''  ESCAPE ''!'' 
				AND type_desc NOT IN (''VIEW'',''USER_TABLE'')
				AND ObjectName NOT LIKE ''',a.ObjectName,''') AS src
					ON tgt.SourceDB=src.SourceDB
						AND tgt.SourceSchema=src.SourceSchema
						AND tgt.sourceObject=src.sourceObject
						AND tgt.usedDB=src.usedDB
						AND tgt.usedSchema=src.usedSchema
						AND tgt.usedTable=src.usedTable
			WHEN NOT MATCHED BY TARGET THEN
			INSERT (SourceDB,SourceSchema,sourceObject,usedDB,usedSchema,usedTable)
				VALUES (src.SourceDB,src.SourceSchema,src.SourceObject,src.usedDB,src.usedSchema,src.usedTable);')

		AS VARCHAR(MAX)),'   ')
		WITHIN GROUP (ORDER BY a.ObjectName ASC)
	FROM #temp_allobjects AS a



EXEC sp_executesql @sql  

--select * from #temp_dependencies



----------------------------------------------------------------
DROP TABLE IF EXISTS #temp_complete
CREATE TABLE #temp_complete(	SourceDB VARCHAR(500)
								,SourceSchema VARCHAR(500)
								,sourceObject VARCHAR(500)
								,usedDB VARCHAR(500)
								,usedSchema VARCHAR(500)
								,usedTable VARCHAR(500)
								)
;WITH cte_recurr AS
	(
		SELECT f.SourceDB
			,f.SourceSchema
			,f.sourceObject
			,f.usedDB
			,f.usedSchema
			,f.usedTable
		--	,1 AS [Level]
		FROM #temp_dependencies AS f
			

	UNION ALL


	SELECT 
			r.SourceDB
			,r.SourceSchema
			,r.sourceObject
			,f.usedDB
			,f.usedSchema
			,f.usedTable
		--	,[Level]+1
		FROM cte_recurr AS r
			JOIN  #temp_dependencies AS f
				ON r.usedDB=f.sourceDB
					AND r.usedSchema=f.sourceSchema
					AND r.usedTable=f.sourceObject
		--WHERE [Level]<10
	
)

INSERT INTO #temp_complete (SourceDB,SourceSchema,sourceObject,usedDB,usedSchema,usedTable)
SELECT DISTINCT SourceDB,SourceSchema,sourceObject,usedDB,usedSchema,usedTable--,[Level]
FROM cte_recurr
OPTION (MAXRECURSION 10)

-------------------------------------------------------------------------
-------------------------------------------------------------------------




DROP TABLE IF EXISTS #temp_working
CREATE TABLE #temp_working (SourceDB VARCHAR(500)
							,SourceSchema VARCHAR(500)
							,SourceObject VARCHAR(500)
							,MyStatus		INT
							,DependenciesUnprocessed INT
							,ObjectType VARCHAR(500))





			INSERT INTO #temp_working(SourceDB,SourceSchema,SourceObject,MyStatus,DependenciesUnprocessed,ObjectType)
					SELECT deps.SourceDB
						,deps.SourceSchema
						,deps.sourceObject
						,fulllist.Unscripted AS MyStatus
						,SUM(depdetails.Unscripted) AS DependenciesUnprocessed
						,fulllist.type_desc AS ObjectType
				FROM #temp_allobjects AS fulllist
					JOIN #temp_complete AS deps								--all the objects and their dependencies returned by the recursion
						ON deps.SourceDB=fulllist.ObjectDB
							AND deps.SourceSchema=fulllist.ObjectSchema
							AND deps.SourceObject=fulllist.ObjectName
					JOIN #temp_allobjects AS depdetails						--information about the dependencies themselves
						ON COALESCE(deps.usedDB,deps.SourceDB)=depdetails.ObjectDB
							AND COALESCE(deps.usedSchema,deps.SourceSchema)=depdetails.ObjectSchema
							AND COALESCE(deps.usedTable,deps.SourceObject)=depdetails.ObjectName
							AND depdetails.ObjectDB IS NOT NULL				--some junk things remain if the view is unrefreshed and broken							
				GROUP BY deps.SourceDB
						,deps.SourceSchema
						,deps.sourceObject
						,fulllist.Unscripted
						,fulllist.type_desc
				HAVING SUM(depdetails.Unscripted)=1											--Only pull the ones that are left with just themselves unscripted



-------------------------------------------------------------------------
/*WHILE EXISTS(SELECT TOP 1 * FROM #temp_working)

	BEGIN
*/
			SELECT count(1) as workingcount,'begin'
			FROM #temp_working

			select * FROM #temp_working AS w
				JOIN #temp_allobjects AS o
					ON w.SourceDB=o.ObjectDB
						AND w.SourceSchema=o.ObjectSchema
						AND w.sourceObject=o.ObjectName
			
			--flip the Unscripted flag for anything that was in the #temp_working table (i.e., had all dependencies taken care of)
			UPDATE o
			SET o.Unscripted=0
			FROM #temp_working AS w					
				JOIN #temp_allobjects AS o
					ON w.SourceDB=o.ObjectDB
						AND w.SourceSchema=o.ObjectSchema
						AND w.sourceObject=o.ObjectName

		select @@ROWCOUNT,'affected'
		select count(1) as c,'after update'  from #temp_allobjects where unscripted=1

			select * FROM #temp_working AS w
				JOIN #temp_allobjects AS o
					ON w.SourceDB=o.ObjectDB
						AND w.SourceSchema=o.ObjectSchema
						AND w.sourceObject=o.ObjectName


/*
			--reset the iterative control for the while loop
			TRUNCATE TABLE #temp_working
			INSERT INTO #temp_working(SourceDB,SourceSchema,SourceObject,MyStatus,DependenciesUnprocessed,ObjectType)
					SELECT deps.SourceDB
						,deps.SourceSchema
						,deps.sourceObject
						,fulllist.Unscripted AS MyStatus
						,SUM(depdetails.Unscripted) AS DependenciesUnprocessed
						,fulllist.type_desc AS ObjectType
				FROM #temp_allobjects AS fulllist
					JOIN #temp_complete AS deps								--all the objects and their dependencies returned by the recursion
						ON deps.SourceDB=fulllist.ObjectDB
							AND deps.SourceSchema=fulllist.ObjectSchema
							AND deps.SourceObject=fulllist.ObjectName
					JOIN #temp_allobjects AS depdetails						--information about the dependencies themselves
						ON COALESCE(deps.usedDB,deps.SourceDB)=depdetails.ObjectDB
							AND COALESCE(deps.usedSchema,deps.SourceSchema)=depdetails.ObjectSchema
							AND COALESCE(deps.usedTable,deps.SourceObject)=depdetails.ObjectName
							AND depdetails.ObjectDB IS NOT NULL				--some junk things remain if the view is unrefreshed and broken							
				GROUP BY deps.SourceDB
						,deps.SourceSchema
						,deps.sourceObject
						,fulllist.Unscripted
						,fulllist.type_desc
				HAVING SUM(depdetails.Unscripted)=1											--Only pull the ones that are left with just themselves unscripted
		--		ORDER BY c.type_desc,r.sourceDB,r.SourceSchema,r.sourceObject

		--
		select count(1) as c,'end' from #temp_working
		select * --count(1) as c,'unscripted'  
		from #temp_allobjects where unscripted=1
	END

	*/

	select * from #temp_dependencies
	where sourceObject='v_MGB_POR_Field_BenchmarkingReportStaging'


--select * from #temp_allobjects where unscripted=1





