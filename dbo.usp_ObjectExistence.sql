

--Only for the current database context
--doesn't work for things like indexes or constraints
--Purpose: doing a drop and recreate loses any history and permissions. this sproc supports a standard synapse-friendly way of creating a stub then just altering it later
					
				--or it can be used for the standard drop and create as a backup method

				/*
				IF NOT EXISTS (SELECT * FROM sys.objects WHERE name = MyProcName AND type = ‘P’)
				BEGIN
					EXEC usp_ObjectExistence('myschema','v_myview','V','CREATE')
				END
				GO
				ALTER PROCEDURE myschema.v_myview( …
				--source: https://sqlstudies.com/2013/02/25/drop-and-create-vs-alter/
*/
	--Synapse standards for "drop if exists":
	--like can't do it for Procedures:https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-procedure-transact-sql?view=sql-server-ver16
		--or synonyms https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-synonym-transact-sql?view=sql-server-ver16
		--or tables (user defined or external) https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-table-transact-sql?view=sql-server-ver16
		--claims that it can for views? https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-view-transact-sql?view=sql-server-ver16
		--...and for functions? https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-function-transact-sql?view=sql-server-ver16
	--OBJECT_SCHEMA_NAME(object_id) works in dedicated but NOT on serverless :/


-----FUTURE ENHANCEMENTS: The function needs to specify if it's scalar or tvf, otherwise if you create the wrong type of stub then the alter will fail

DECLARE @schema VARCHAR(100)='reporting'
DECLARE @objectname VARCHAR(1000)='v_DimDevicesLocCustVaultBank'
DECLARE @objecttype VARCHAR(100)='V'
DECLARE @action VARCHAR(50)='CREATE' --1. creates a stub that can be altered in a second step, 2. drop to be followed by create in a second step

IF @action='CREATE' AND @objecttype IN('S','T') 
	PRINT 'Does not support altering this type of object'

--DROP TABLE #objecttypes
--CREATE TABLE #objecttypes (syscode VARCHAR(5),sysobjectname VARCHAR(50),Parametercode VARCHAR(5),commandname VARCHAR(50))


DECLARE @syscode VARCHAR(5)
DECLARE @commandname VARCHAR(50)

--INSERT INTO #objecttypes(syscode,sysobjectname,Parametercode,commandname)
SELECT --DISTINCT 
@syscode =o.type --AS syscode
		--,o.type_desc AS sysobjectname
		--,CASE WHEN ex.Object_id IS NOT NULL THEN 'E' --external table
		--	WHEN o.type_desc LIKE '%FUNCTION' THEN 'F'
		--	WHEN o.type_desc LIKE '%TABLE' THEN 'T'
		--	WHEN o.type_desc LIKE 'SQL_STORED_PROCEDURE' THEN 'P'
		--	WHEN o.type_desc LIKE 'SYNONYM' THEN 'S'
		--	WHEN o.type_desc LIKE 'VIEW' THEN 'V'
		--	END AS Parametercode
		,@commandname =CASE WHEN ex.Object_id IS NOT NULL THEN 'EXTERNAL TABLE'
			WHEN o.type_desc LIKE '%FUNCTION' THEN 'FUNCTION'
			WHEN o.type_desc LIKE 'USER_TABLE' THEN 'TABLE'
			WHEN o.type_desc LIKE 'SQL_STORED_PROCEDURE' THEN 'PROCEDURE'
			WHEN o.type_desc LIKE 'SYNONYM' THEN 'SYNONYM'
			WHEN o.type_desc LIKE 'VIEW' THEN 'VIEW'
			END --AS commandname
		FROM sys.objects AS o
			LEFT JOIN sys.external_tables AS ex ON o.object_id=ex.object_id
	WHERE @objecttype =CASE WHEN ex.Object_id IS NOT NULL THEN 'E' --external table
								WHEN o.type_desc LIKE '%FUNCTION' THEN 'F'
								WHEN o.type_desc LIKE '%TABLE' THEN 'T'
								WHEN o.type_desc LIKE 'SQL_STORED_PROCEDURE' THEN 'P'
								WHEN o.type_desc LIKE 'SYNONYM' THEN 'S'
								WHEN o.type_desc LIKE 'VIEW' THEN 'V'
								END
		AND o.object_id=OBJECT_ID(CONCAT(@schema,'.',@objectname),o.type)
--------------------------------------------------------------------
--Error check here for valid objecttypes
--IF @syscode IS NULL PRINT 'Error: not a valid objecttype'

--Error check for valid actions
IF @action NOT IN ('CREATE','DROP') PRINT 'Error: not a valid action'


----------------------------------------------

--reference: https://sqlstudies.com/2013/02/25/drop-and-create-vs-alter/
	DECLARE @sql NVARCHAR(MAX)=''

	IF @action='DROP' AND @commandname IS NOT NULL

		BEGIN
			--Drop commands are standard for all objects
			SET @sql=CONCAT('DROP ',@commandname, ' ',@schema,'.',@objectname)

		END
	ELSE IF @action='CREATE' AND @commandname IS NULL --Create the stub
		BEGIN
		--does not support tables (external or otherwise) or synonyms
			@SQL=CONCAT(N'CREATE ',@commandname, ' ', @schema,'.',@objectname,' AS SELECT 1 AS Temp') --for views and sprocs
	
		--CREATE FUNCTION dbo.myfunction AS RETURN 1 ? --create IF or TF for tvf,FN for scalar
			--TVF:
				--CREATE FUNCTION dbo.myfunction(@i INT) RETURNS TABLE AS RETURN ( SELECT 1 AS Temp,2 AS Temp2));
			--scalar:
				--CREATE FUNCTION dbo.myfunction(@i INT) RETURNS INT AS BEGIN RETURN @i; END;
	
			
		END


	select @sql  --EXEC sp_executesql @sql



