
DROP TABLE IF EXISTS #temp_AllObjects
DROP TABLE IF EXISTS #temp_db
DROP TABLE IF EXISTS #temp_dep
DROP TABLE IF EXISTS #temp_firstlevel
DROP TABLE IF EXISTS #temp_analytics
DROP TABLE IF EXISTS #temp_mgr
DROP TABLE IF EXISTS #temp_syn
DROP TABLE IF EXISTS #full
DROP TABLE IF EXISTS #fullcolumns




SELECT TRY_CAST(database_id AS VARCHAR(MAX)) AS [database_id_varchar],database_id AS database_id_int, [name] AS [database_name]
INTO #temp_db
FROM sys.databases WITH (NOLOCK)
WHERE name IN ('C360Analytics','C360MGR_Dev')



--Need to find entire list of all objects we may be interested in, to 1. correct the synonyms, and 2. account for objects that may not have referencing entities
	--as found in later step

--Pull all objects in the analytics db that we'd be interested in

	SELECT 'C360Analytics' AS ObjectDB
	,OBJECT_SCHEMA_NAME(object_id) AS ObjectSchema,
		name AS ObjectName
		,type_desc  collate SQL_Latin1_General_CP1_CI_AS AS ObjectType
		,object_id
	INTO #temp_analytics
	FROM C360Analytics.sys.all_objects
	WHERE (type_desc LIKE '%function%'
			OR type_desc LIKE '%procedure%'
			OR type_desc LIKE '%table%'
			OR type_desc ='VIEW')
		AND is_ms_shipped <>1  --don't want synonyms, those are added in better detail later

	



--Next, pull all objects we might be interested in the other DB

	SELECT 'C360MGR_Dev' AS ObjectDB
		,OBJECT_SCHEMA_NAME(object_id,(SELECT database_id_int FROM #temp_db WHERE database_name='C360MGR_Dev')) AS ObjectSchema
		,name AS ObjectName
		,COALESCE(type_desc  collate SQL_Latin1_General_CP1_CI_AS,'SYNONYM') AS ObjectType
		,object_id 
	INTO #temp_mgr
	FROM C360MGR_Dev.sys.all_objects
	WHERE (type_desc LIKE '%function%'
			OR type_desc LIKE '%procedure%'
			OR type_desc LIKE '%table%'
			OR type_desc  IN ('VIEW','SYNONYM')) --for some reason synonyms aren't visible in this db, must be permissions. Left them in anyway
		AND is_ms_shipped <>1 



--Get object info for the synonyms
--Assumptions= Analytics DB won't have synonyms pointing within itself. Only care about synonyms pointing to MGR so leave out anything else for now
--Some have blank TargetType and I can't find them in the DB manually. Maybe they're synonyms I'm blocked from seeing?


	SELECT 'C360Analytics' AS SynDB
		,OBJECT_SCHEMA_NAME(s.object_id) AS SynSchema
		,s.name AS SynObjectName
		,'C360MGR_Dev' AS TargetDB
		,COALESCE (PARSENAME (base_object_name, 2), SCHEMA_NAME (SCHEMA_ID ())) AS TargetSchema --resolve nulls to be same as synonym schema. That's the best that can be done. 
		,PARSENAME (base_object_name, 1) AS TargetObject
		,m.ObjectType  collate SQL_Latin1_General_CP1_CI_AS AS TargetType
	INTO #temp_syn
	FROM C360Analytics.sys.synonyms AS s
		LEFT JOIN #temp_mgr AS m 
			ON COALESCE (PARSENAME (s.base_object_name, 2), SCHEMA_NAME (SCHEMA_ID ()))=m.ObjectSchema
				AND PARSENAME (base_object_name, 1)=m.ObjectName
	WHERE COALESCE (PARSENAME (base_object_name, 3), DB_NAME (DB_ID ())) ='C360MGR_Dev'
	


--Need a list of all objects available, since not all of them will have dependencies (which will be calculated in the next step)
SELECT ObjectDB,ObjectSchema,ObjectName,ObjectType,object_id 
INTO #temp_AllObjects
FROM #temp_analytics

UNION ALL
SELECT ObjectDB,ObjectSchema,ObjectName,ObjectType,object_id 
FROM #temp_mgr

UNION ALL

SELECT synDB,SynSchema,SynObjectName,'SYNONYM',NULL
FROM #temp_syn


		




/*********************************************
Account for synonym redirections
*********************************************/



 --coalescing to dbo for schemas since I'm not entirely sure what they should be and I want tos ee if that helps the inner join
SELECT 
	'C360Analytics' AS SourceDB
	,OBJECT_SCHEMA_NAME(dep.referencing_id,(SELECT database_id_varchar from #temp_db WHERE [database_name]='C360Analytics')) AS SourceSchema
        ,OBJECT_NAME(referencing_id,(SELECT database_id_int from #temp_db WHERE [database_name]='C360Analytics')) AS SourceObject
        ,CAST(NULL AS NVARCHAR(50)) AS object_type
		,COALESCE(referenced_database_name, 'C360Analytics') AS usesDB
        ,COALESCE(OBJECT_SCHEMA_NAME(referenced_id,(SELECT database_id_varchar from #temp_db WHERE [database_name]=COALESCE(referenced_database_name, 'C360Analytics'))) ,'dbo') AS usesSchema
        ,referenced_entity_name  AS usesEntity
		,CAST(NULL AS NVARCHAR(50))  AS uses_type --far easier to get type from the sys.all_objects temp tables instead
INTO #temp_dep 
from 
	C360Analytics.sys.sql_expression_dependencies as dep WITH (NOLOCK)



UNION ALL

SELECT 
	'C360MGR_Dev' AS SourceDB
	,OBJECT_SCHEMA_NAME(dep.referencing_id,(SELECT database_id_varchar from #temp_db WHERE [database_name]='C360MGR_Dev')) AS SourceSchema
        ,OBJECT_NAME(referencing_id,(SELECT database_id_int from #temp_db WHERE [database_name]='C360MGR_Dev')) AS SourceObject
        ,NULL AS object_type
		,COALESCE(referenced_database_name, 'C360MGR_Dev') AS usesDB
        ,OBJECT_SCHEMA_NAME(referenced_id,(SELECT database_id_varchar from #temp_db WHERE [database_name]=COALESCE(referenced_database_name, 'C360MGR_Dev')))  AS usesSchema
        ,referenced_entity_name  AS usesEntity
		,NULL AS uses_type
FROM
	C360MGR_Dev.sys.sql_expression_dependencies as dep WITH (NOLOCK)


UNION ALL

--These are essentially dependencies, and we'll need them for the recursion but exclude from final results
SELECT s.SynDB,s.SynSchema,s.SynObjectName,'SYNONYM' AS synOb,s.TargetDB,s.TargetSchema,s.TargetObject,s.Targettype
FROM	#temp_syn AS s







UPDATE #temp_dep
	SET object_type=
		CASE WHEN a.ObjectDB='C360MGR_Dev' THEN COALESCE(a.ObjectType,'SYNONYM') ELSE a.ObjectType END
FROM #temp_dep AS d
	JOIN #temp_AllObjects AS a
		ON a.ObjectDB=d.SourceDB
			AND a.ObjectSchema=d.SourceSchema
			AND a.ObjectName=d.sourceObject


UPDATE #temp_dep
	SET uses_type=
		CASE WHEN a.ObjectDB='C360MGR_Dev' THEN COALESCE(a.ObjectType,'SYNONYM') ELSE a.ObjectType END
FROM #temp_dep AS d
	JOIN #temp_AllObjects AS a
		ON a.ObjectDB=d.usesDB
			AND a.ObjectSchema=d.usesSchema
			AND a.ObjectName=d.usesEntity



UPdATE #temp_dep
	SET usesDB=NULL
		,usesSchema=NULL
		,usesEntity=NULL
		,uses_type=NULL
FROM #temp_dep
	where usesDB=SourceDB
		AND usesSchema=SourceSchema
		AND usesEntity=SourceObject




/* ********************************
recursion through all the levels of dependencies 
**********************************/

--first build out the full first level
select DISTINCT

	ao.ObjectDB
	,ao.ObjectSchema
	,ao.ObjectName
	,ao.ObjectType
	,dep.usesDB
	,dep.usesSchema
	,dep.usesEntity
	,dep.uses_type

INTO #temp_firstlevel
from #temp_AllObjects AS ao
	LEFT JOIN #temp_dep AS dep
		ON ao.ObjectDB=dep.SourceDB
			AND ao.ObjectSchema=dep.SourceSchema
			AND ao.ObjectName=dep.sourceObject
	order by ObjectDB,ObjectSchema,ObjectName





;WITH

cte_recurr AS
	(
		SELECT ObjectDB
			,ObjectSchema
			,ObjectName
			,ObjectType
			,usesDB
			,usesSchema
			,usesEntity
			,uses_type

		FROM #temp_firstlevel AS f


	UNION ALL

			SELECT  
					r.ObjectDB
					,r.ObjectSchema
					,r.ObjectName
					,r.ObjectType
					,f.usesDB
					,f.usesSchema
					,f.usesEntity
					,f.uses_type
				FROM #temp_firstlevel AS f 
					JOIN  cte_recurr AS r 
						ON 
							r.usesDB=f.ObjectDB
							AND r.usesSchema= f.ObjectSchema
							AND r.usesEntity= f.ObjectName


	
)


select DISTINCT 
	ObjectDB
	,ObjectSchema
	,ObjectName
	,ObjectType
	,CAST(CONCAT(objectdb,'.',objectschema ,'.',objectname ) AS VARCHAR(1000)) AS ObjectFullName
	,usesDB
	,usesSchema
	,usesEntity
	,uses_type
	,CAST(CONCAT(
		COALESCE(usesDB,objectdb),'.'
		,COALESCE(usesSchema,objectschema) ,'.'
		,COALESCE(usesEntity,objectname) ) AS VARCHAR(1000)) AS UsesObjectFullName

INTO #full
from cte_recurr


----------------Now correct for the fact tables. Can't do this earlier because it'd get into an infinite loop
--it essentially creates a dependency link between the fact tables and what objects they SHOULD have "officially" used (as opposed to just being mentioned in an ETL somewhere)

INSERT INTO #full (ObjectDB,ObjectSchema,ObjectName,ObjectType,ObjectFullName,usesDB,usesSchema,usesEntity,UsesObjectFullName,uses_type)
SELECT ao.ObjectDB
	,ao.ObjectSchema
	,ao.ObjectName
	,ao.ObjectType
	,CAST(CONCAT(ao.objectdb,'.',ao.objectschema ,'.',ao.objectname ) AS VARCHAR(1000)) AS ObjectFullName
	,COALESCE(deps.usesDB,deps.objectdb) AS usesDB
	,COALESCE(deps.usesSchema,deps.objectschema) AS usesSchema
	,COALESCE(deps.usesEntity,deps.objectname) AS usesEntity
	,CAST(CONCAT(
		COALESCE(deps.usesDB,deps.objectdb),'.'
		,COALESCE(deps.usesSchema,deps.objectschema) ,'.'
		,COALESCE(deps.usesEntity,deps.objectname) ) AS VARCHAR(1000)) AS fullobjectlocation
	,COALESCE(deps.uses_type,deps.objecttype) AS uses_type	
FROM #temp_AllObjects AS ao --using this to just get a distinct list of objects, don't care about all the other details right now. Just need the objectname
	JOIN #full AS deps 
		ON  deps.ObjectName LIKE '%' + ao.ObjectName  --seems to be the usual naming convention
			AND ao.ObjectDB='C360Analytics' AND ao.ObjectSchema='Fact' AND ao.ObjectType='USER_TABLE'  --usually tables don't have any dependencies, BUT these fact tables do so we need to link that up
			AND (deps.objectname LIKE '%Extract%' OR deps.ObjectName LIKE '%Merge%') 
			AND (deps.ObjectType LIKE '%PROCEDURE' OR deps.ObjectType LIKE 'VIEW')









--============================final full return plus columns
--Can't return any procedure fields because most use a temp table and that isn't supported by the various sys functions 
	--sys.dm_exec_describe_first_result_set, sys.dm_exec_describe_first_result_set_for_object,sp_describe_first_result_set. And openrowset requires certain permissions
select f.ObjectDB
	,f.ObjectSchema
	,f.ObjectName,f.ObjectFullName
	,f.ObjectType
	,COALESCE(f.usesDB,'') AS usesDB--so that the excel import is pretty
	,COALESCE(f.usesSchema,'') AS usesSchema
	,COALESCE(f.usesEntity,'') AS usesEntity
	,f.UsesObjectFullName
	,COALESCE(f.uses_type,'') AS uses_type
	,COALESCE(ana.COLUMN_NAME,mgr.COLUMN_NAME,'') AS UsesColumnName
INTO #fullcolumns
FROM #full AS f
	LEFT JOIN C360Analytics.INFORMATION_SCHEMA.COLUMNS AS ana
		ON f.usesDB='C360Analytics'
			AND f.usesSchema=ana.TABLE_SCHEMA
			AND f.usesEntity=ana.TABLE_NAME
	LEFT JOIN C360MGR_Dev.INFORMATION_SCHEMA.COLUMNS AS mgr
		ON f.usesDB='C360MGR_Dev'
			AND f.usesSchema=mgr.TABLE_SCHEMA
			AND f.usesEntity=mgr.TABLE_NAME
WHERE NOT(uses_type ='SYNONYM' AND ObjectDB='C360Analytics')

---------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #reports
CREATE TABLE #reports (reportname VARCHAR(500))
INSERT INTO #reports (reportname)
SELECT 'Administration\Column Definitions' AS report UNION ALL
SELECT 'Administration\Customer Register Drilldown' AS report UNION ALL
SELECT 'Administration\Customers' AS report UNION ALL
SELECT 'Administration\Environment' AS report UNION ALL
SELECT 'Administration\Latest Customer Report Activity' AS report UNION ALL
SELECT 'Administration\Latest Report Activity' AS report UNION ALL
SELECT 'Administration\Latest Report Activity by Report' AS report UNION ALL
SELECT 'Administration\Latest User Summary' AS report UNION ALL
SELECT 'Administration\Report Column Definitions' AS report UNION ALL
SELECT 'Administration\Report Metrics' AS report UNION ALL
SELECT 'Administration\Report Usage' AS report UNION ALL
SELECT 'Administration\Report Usage by Group' AS report UNION ALL
SELECT 'Administration\Report Usage By Month' AS report UNION ALL
SELECT 'Administration\Role Users' AS report UNION ALL
SELECT 'Administration\Service Log Recap' AS report UNION ALL
SELECT 'Administration\Service Logs' AS report UNION ALL
SELECT 'Administration\Service Task Details' AS report UNION ALL
SELECT 'Administration\Subscription Usage' AS report UNION ALL
SELECT 'Administration\Users or Roles' AS report UNION ALL
SELECT 'Bank Branch Management\Cash Usage' AS report UNION ALL
SELECT 'Bank Branch Management\Deposit Cassette Removal' AS report UNION ALL
SELECT 'Bank Branch Management\Device Deposit Contents' AS report UNION ALL
SELECT 'Bank Branch Management\End Of Day Branch Cash Balance' AS report UNION ALL
SELECT 'Bank Branch Management\External Vault Content' AS report UNION ALL
SELECT 'Bank Branch Management\Shipment(s) Received from Carrier' AS report UNION ALL
SELECT 'Bank Transmission\Bank Transmission Match' AS report UNION ALL
SELECT 'Bank Transmission\BI Transmissions Alert' AS report UNION ALL
SELECT 'Bank Transmission\EOD Bank Transmission Detail' AS report UNION ALL
SELECT 'Bank Transmission\Missing Bank Acknowledgment Alert' AS report UNION ALL
SELECT 'Bank Transmission\Missing EOD Bank Transmissions' AS report UNION ALL
SELECT 'Bank Transmission\Provisional Credit' AS report UNION ALL
SELECT 'Bank Transmission\ProvisionalCreditReportDetail' AS report UNION ALL
SELECT 'Bank Transmission\ProvisionalCreditReportDetail2' AS report UNION ALL
SELECT 'Carrier Optimization\Bank Optimization Map' AS report UNION ALL
SELECT 'Carrier Optimization\Carrier Optimization Map' AS report UNION ALL
SELECT 'Carrier Optimization\Carrier Optimization Map by State' AS report UNION ALL
SELECT 'Carrier Optimization\Carrier Optimization Report' AS report UNION ALL
SELECT 'Carrier\Bank-Owned CIT' AS report UNION ALL
SELECT 'Carrier\CIT Location Validation' AS report UNION ALL
SELECT 'Carrier\CIT Scheduled Services' AS report UNION ALL
SELECT 'Carrier\CIT Service Performance' AS report UNION ALL
SELECT 'Carrier\CIT Service Performance By Login' AS report UNION ALL
SELECT 'Carrier\CIT Visits - Customer Level ' AS report UNION ALL
SELECT 'Carrier\Courier Device Time' AS report UNION ALL
SELECT 'Carrier\Days Of Service Optimization' AS report UNION ALL
SELECT 'Carrier\Days Of Service Optimization 2.0' AS report UNION ALL
SELECT 'Carrier\Deposit Cassette Full Alert Report' AS report UNION ALL
SELECT 'Carrier\Deposit Rate' AS report UNION ALL
SELECT 'Carrier\Electronic Manifest' AS report UNION ALL
SELECT 'Carrier\Enhanced Pickup' AS report UNION ALL
SELECT 'Carrier\OutStanding Empties' AS report UNION ALL
SELECT 'Carrier\Transportation Carrier Invoice Validation' AS report UNION ALL
SELECT 'Clears\All Cases' AS report UNION ALL
SELECT 'Clears\Claim Status' AS report UNION ALL
SELECT 'Clears\Closed Cases' AS report UNION ALL
SELECT 'Clears\Open Cases' AS report UNION ALL
SELECT 'Coin Shortage Analysis\Cash Depletion' AS report UNION ALL
SELECT 'Coin Shortage Analysis\Content Depletion' AS report UNION ALL
SELECT 'Dart\Business Unit Site Survey Dashboard' AS report UNION ALL
SELECT 'Dart\Installation Ready' AS report UNION ALL
SELECT 'Dart\Installation Status' AS report UNION ALL
SELECT 'Dart\Installations' AS report UNION ALL
SELECT 'Dart\Installed Serial Numbers' AS report UNION ALL
SELECT 'Dart\Locations' AS report UNION ALL
SELECT 'Dart\Modifications' AS report UNION ALL
SELECT 'Dart\Planning' AS report UNION ALL
SELECT 'Dart\Purchase Order' AS report UNION ALL
SELECT 'Dart\Risers' AS report UNION ALL
SELECT 'Dart\Shipment Status' AS report UNION ALL
SELECT 'Dart\Site Schedule' AS report UNION ALL
SELECT 'Dart\Site Survey Dashboard' AS report UNION ALL
SELECT 'Dart\Smart Safe Stats' AS report UNION ALL
SELECT 'Dart\Smart Safe Survey Billing' AS report UNION ALL
SELECT 'Dart\Survey Comments' AS report UNION ALL
SELECT 'Dart\Survey Modifications' AS report UNION ALL
SELECT 'Dart\Surveys Missed' AS report UNION ALL
SELECT 'Dart\Surveys Rejected' AS report UNION ALL
SELECT 'Dart\Surveys ReScheduled' AS report UNION ALL
SELECT 'Dart\Surveys Scheduled' AS report UNION ALL
SELECT 'Dart\Surveys Scheduled Calendar' AS report UNION ALL
SELECT 'Dart\Surveys To Approve' AS report UNION ALL
SELECT 'Dart\Surveys to Reschedule' AS report UNION ALL
SELECT 'Dart\Tidel Installation Confirmation' AS report UNION ALL
SELECT 'Dart\Workflow' AS report UNION ALL
SELECT 'Dart\Workflow History' AS report UNION ALL
SELECT 'Device\Accounting Day Error' AS report UNION ALL
SELECT 'Device\Daily Alerts' AS report UNION ALL
SELECT 'Device\Device Details' AS report UNION ALL
SELECT 'Device\Device Go Live Date' AS report UNION ALL
SELECT 'Device\Device Jams' AS report UNION ALL
SELECT 'Device\Device Offline' AS report UNION ALL
SELECT 'Device\Device Uptime' AS report UNION ALL
SELECT 'Device\External Vault Audit' AS report UNION ALL
SELECT 'Device\Installation QC' AS report UNION ALL
SELECT 'Device\Installed Devices' AS report UNION ALL
SELECT 'Device\Last Cleaning' AS report UNION ALL
SELECT 'Device\Last Device Activity' AS report UNION ALL
SELECT 'Device\Last Device Audit' AS report UNION ALL
SELECT 'Device\Missing Safe Connect Transmissions' AS report UNION ALL
SELECT 'Device\Robbery Protocol' AS report UNION ALL
SELECT 'Device\Service Cash' AS report UNION ALL
SELECT 'Device\Unknown Logins' AS report UNION ALL
SELECT 'Exports\Coin Shortage Detail By Day' AS report UNION ALL
SELECT 'Exports\Export Deposit Data By CIT Activity' AS report UNION ALL
SELECT 'Exports\External Vault Balancing Report' AS report UNION ALL
SELECT 'Exports\RoundingRecommendations' AS report UNION ALL
SELECT 'Exports\RoundingRecommendationsForTaskForce' AS report UNION ALL
SELECT 'Exports\RoundingRecommendationsOff' AS report UNION ALL
SELECT 'Exports\RoundingRecommendationsOn' AS report UNION ALL
SELECT 'Exports\RoundingRecommendationsPOSAdjust' AS report UNION ALL
SELECT 'Exports\RoundingRecommendationsPOSFormat' AS report UNION ALL
SELECT 'Exports\RoundingRecommendationsReport' AS report UNION ALL
SELECT 'Exports\RoundingRecommendationsSendToPOS' AS report UNION ALL
SELECT 'Exports\Store Contents' AS report UNION ALL
SELECT 'Exports\Store PaidInOut' AS report UNION ALL
SELECT 'Exports\Store Register' AS report UNION ALL
SELECT 'Internal Management\Business Continuity Report' AS report UNION ALL
SELECT 'InternalDashboards\BankTransmission' AS report UNION ALL
SELECT 'InternalDashboards\Score Card' AS report UNION ALL
SELECT 'InternalDashboards\SmartQ  Transmission Details' AS report UNION ALL
SELECT 'InternalDashboards\WalmartDataFlow' AS report UNION ALL
SELECT 'InternalDashboards\WMT and SC DataFlow' AS report UNION ALL
SELECT 'Inventory\AS400 WinCo' AS report UNION ALL
SELECT 'Inventory\Auto Order Buffers' AS report UNION ALL
SELECT 'Inventory\Bank Cash Balances by Store' AS report UNION ALL
SELECT 'Inventory\Cash In Out Net' AS report UNION ALL
SELECT 'Inventory\Cash Position' AS report UNION ALL
SELECT 'Inventory\Cash Status Report' AS report UNION ALL
SELECT 'Inventory\Cash Usage' AS report UNION ALL
SELECT 'Inventory\Cash Usage Dashboard' AS report UNION ALL
SELECT 'Inventory\Cash Utilization Pivot' AS report UNION ALL
SELECT 'Inventory\CFT Deposit Pickup Flow' AS report UNION ALL
SELECT 'Inventory\Change Order Delivery' AS report UNION ALL
SELECT 'Inventory\Change Order Delivery by Denomination' AS report UNION ALL
SELECT 'Inventory\Change Order Discrepancies' AS report UNION ALL
SELECT 'Inventory\Change Order Discrepancy by Denomination' AS report UNION ALL
SELECT 'Inventory\Check And Cash' AS report UNION ALL
SELECT 'Inventory\Check Transactions and Deposits' AS report UNION ALL
SELECT 'Inventory\Check Transactions By Register' AS report UNION ALL
SELECT 'Inventory\contenu utilisable en fin de journ‚e par d‚nomination' AS report UNION ALL
SELECT 'Inventory\Current Content' AS report UNION ALL
SELECT 'Inventory\Customer Register Drilldown' AS report UNION ALL
SELECT 'Inventory\Daily Deposit' AS report UNION ALL
SELECT 'Inventory\Deposit Slip Line' AS report UNION ALL
SELECT 'Inventory\Device Deposit Contents' AS report UNION ALL
SELECT 'Inventory\Device Deposit Contents By Area' AS report UNION ALL
SELECT 'Inventory\Door Activity' AS report UNION ALL
SELECT 'Inventory\Emergency Fund Report' AS report UNION ALL
SELECT 'Inventory\End Of Day Usable Content By Area' AS report UNION ALL
SELECT 'Inventory\End Of Day Usable Content By Denomination' AS report UNION ALL
SELECT 'Inventory\Enregistrer une activit‚ par d‚nomination' AS report UNION ALL
SELECT 'Inventory\EOD Balancing' AS report UNION ALL
SELECT 'Inventory\External Vault Content' AS report UNION ALL
SELECT 'Inventory\Goodwill Integration' AS report UNION ALL
SELECT 'Inventory\Inventory Guard History Report' AS report UNION ALL
SELECT 'Inventory\Inventory Guard Report' AS report UNION ALL
SELECT 'Inventory\Inventory Trend' AS report UNION ALL
SELECT 'Inventory\K Bank Deposits' AS report UNION ALL
SELECT 'Inventory\Location Cash Out' AS report UNION ALL
SELECT 'Inventory\Low Inventory' AS report UNION ALL
SELECT 'Inventory\Low Inventory V2' AS report UNION ALL
SELECT 'Inventory\Low Inventory V2 - With Out Detail' AS report UNION ALL
SELECT 'Inventory\Moved to Deposit Cassette' AS report UNION ALL
SELECT 'Inventory\Operating Fund Report' AS report UNION ALL
SELECT 'Inventory\Pickup' AS report UNION ALL
SELECT 'Inventory\Porto''s Till Net' AS report UNION ALL
SELECT 'Inventory\Ramasser' AS report UNION ALL
SELECT 'Inventory\Register Activity' AS report UNION ALL
SELECT 'Inventory\Register Activity By Denomination' AS report UNION ALL
SELECT 'Inventory\Register Cash Usage' AS report UNION ALL
SELECT 'Inventory\Register Optimization' AS report UNION ALL
SELECT 'Inventory\Register Optimization Advanced' AS report UNION ALL
SELECT 'Inventory\Report1' AS report UNION ALL
SELECT 'Inventory\Robbery Alert' AS report UNION ALL
SELECT 'Inventory\SCO Balance' AS report UNION ALL
SELECT 'Inventory\Self-Checkout Transactions' AS report UNION ALL
SELECT 'Inventory\Smart Safe Employee Activity Detail' AS report UNION ALL
SELECT 'Inventory\Smart Safe Employee Activity Summary' AS report UNION ALL
SELECT 'Inventory\Till Optimization' AS report UNION ALL
SELECT 'Inventory\Under OpFund' AS report UNION ALL
SELECT 'Inventory\utilisation d''argent' AS report UNION ALL
SELECT 'Inventory\Vault Drops' AS report UNION ALL
SELECT 'Inventory\Vault Fund In Out Net Activity' AS report UNION ALL
SELECT 'Inventory\Virtual Checkin By Denomination' AS report UNION ALL
SELECT 'Inventory\Walmart 2021 Tax LIR' AS report UNION ALL
SELECT 'Loss Prevention\Advance Times' AS report UNION ALL
SELECT 'Loss Prevention\Advances' AS report UNION ALL
SELECT 'Loss Prevention\Checkout Times' AS report UNION ALL
SELECT 'Loss Prevention\Loss Prevention Dashboard' AS report UNION ALL
SELECT 'Loss Prevention\Notes Remaining' AS report UNION ALL
SELECT 'Loss Prevention\Service Cash Transactions' AS report UNION ALL
SELECT 'Loss Prevention\Transaction Ratios' AS report UNION ALL
SELECT 'Loss Prevention\Vault Drops' AS report UNION ALL
SELECT 'PowerBI\Device Metrics' AS report UNION ALL
SELECT 'PowerBI\Inventory Management' AS report UNION ALL
SELECT 'PowerBI\Loss Prevention / Asset Protection' AS report UNION ALL
SELECT 'PowerBI\Store Activity' AS report UNION ALL
SELECT 'Reconciliation\Bank Transactions' AS report UNION ALL
SELECT 'Reconciliation\BI Transmission Averages' AS report UNION ALL
SELECT 'Reconciliation\BI VeriBalance Carry Forward' AS report UNION ALL
SELECT 'Reconciliation\Change Order Purchase' AS report UNION ALL
SELECT 'Reconciliation\DRecords' AS report UNION ALL
SELECT 'Reconciliation\Empty Processing Time' AS report UNION ALL
SELECT 'Reconciliation\Funds Extracted' AS report UNION ALL
SELECT 'Reconciliation\Funds Extracted_Globalization' AS report UNION ALL
SELECT 'Reconciliation\Kroger Reconciliation' AS report UNION ALL
SELECT 'Reconciliation\Machine Out-Of-Balance' AS report UNION ALL
SELECT 'Reconciliation\Net Coin and Cash Reconciliation' AS report UNION ALL
SELECT 'Reconciliation\Provisional Credit Cutover Report' AS report UNION ALL
SELECT 'Reconciliation\Provisional Credit On Deposit Cassette' AS report UNION ALL
SELECT 'Reconciliation\Recycler Balancing' AS report UNION ALL
SELECT 'Reconciliation\Recycler BalancingV1' AS report UNION ALL
SELECT 'Reconciliation\SCO Fund' AS report UNION ALL
SELECT 'Reconciliation\Smart Safe Device Funds' AS report UNION ALL
SELECT 'Reconciliation\Till Balancing' AS report



