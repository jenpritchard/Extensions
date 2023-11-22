/*CREATE FUNCTION [dbo].[udf_CleanStrings](@S varchar(max))

   RETURNS VARCHAR(MAX)

AS

BEGIN
DECLARE @t AS VARCHAR(MAX)

;WITH    cte1 (N) As (SELECT 1 FROM (Values (1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) N(N))
			,cte2 (R) AS (SELECT ROW_NUMBER()  OVER (ORDER BY (SELECT NULL))-1 FROM cte1 a CROSS JOIN cte1 b)
			,cte3 (r,c) AS (SELECT r,CHAR(R) FROM cte2  WHERE NOT R BETWEEN 32 AND 126)
SELECT @t=  trim(replace(replace(replace(Replace(@S,C,''),' ','†‡'),'‡†',''),'†‡',' ')) FROM  cte3

RETURN (@t)

END

*/
--------------------------------------------
;WITH    cte1 (N) As (SELECT 1 FROM (Values (1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) N(N))
			,cte2 (R) AS (SELECT ROW_NUMBER()  OVER (ORDER BY (SELECT NULL))-1 FROM cte1 a CROSS JOIN cte1 b)
			,cte3 (r,c) AS (SELECT r,CHAR(R) AS c FROM cte2  WHERE NOT R BETWEEN 32 AND 126)	
SELECT     
	cast(COALESCE(b.Id,-1) as BIGINT) as BankId,
	cast(COALESCE(b.ShortName,'Unknown') as nvarchar(50)) as ShortName,
	cast(COALESCE(b.Name,'Unknown') as nvarchar(50)) as Name,
	cast(bd.Address1 as nvarchar(100)) as Address1,
	cast(bd.Address2 as nvarchar(100)) as Address2,
	cast(bd.City as nvarchar(50)) as City,
	cast(bd.State as nvarchar(50)) as State,
	cast(bd.PostalCode as nvarchar(50)) as PostalCode,
	cast(bd.CountryCode as nvarchar(50)) as CountryCode,
	cast(TRIM(REPLACE(
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(
												REPLACE(
													REPLACE(COALESCE(bd.timezone,'Unknown'),NCHAR(11),'')
												,NCHAR(12),'')
											,NCHAR(9),'')
										,NCHAR(10),'')
									,NCHAR(13),'')
								,NCHAR(160),'')
							,NCHAR(8201),'')
						,NCHAR(8204),'')
					,NCHAR(8205),'')
				,NCHAR(8206),'')
			,NCHAR(8207),'')) as nvarchar(50)) as TimeZone,
	cast(bd.PhoneNumber as nvarchar(50)) as PhoneNumber,
	cast('U' as nvarchar(1)) as Active,
	CAST(null as varchar(50)) as CreatedBy,
	CAST(null as [datetime2](7))  as DateCreatedUTC,
	cast(b.UpdatedBy as varchar(50)) as UpdatedBy,
	cast(b.DateUpdated as [datetime2](7)) as DateUpdatedUTC 
FROM enrichedzone.Banks b
	LEFT JOIN  
	 (SELECT DISTINCT * FROM [enrichedzone].[Banks_Detail]) AS bd ON b.Id=bd.BankId --distinct is temporary until we get it replicated without duplicates
 