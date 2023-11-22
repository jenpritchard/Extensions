/*=============================================================================
	Author:			steveb
	Create date:	20230308
	Description:	Generates column row hash SQL snippet using HASHBYTES()
					over a concatenation of non-key column values.

	Installation:	Create function in database(s) where target(s) exist.

	Parameters:		@ALGO VARCHAR(8) - HASHBYTES() supported algorithms: MD2, MD4, MD5, SHA, SHA1, SHA2_256(default), SHA2_512
					@SCHEMA NVARCHAR(128) - Schema name of target table
					@TABLE NVARCHAR(128) - Table name of target table
					@EXCLUDE NVARCHAR(MAX) - Column(s) to exclude from hash calculation
						Note: use a dummy value like 'no_pk' to hash all columns

	Usage:			SELECT [dbo].[ufn_genHashCol]('<ALGO>','<SCHEMA>','<TABLE>','<PK>')

	Examples:		/* hash excluding 'ride_id' using SHA1 */
					SELECT [CapitalBikeShare].[dbo].[ufn_genHashCol]('SHA1','dbo','tripdata','ride_id')
					/* hash excluding Latitude and Longitude using MD5 */
					SELECT [WorkDB].[dbo].[ufn_genHashCol]('MD5','dbo','cities','Latitude,Longitude')
					/* hash all columns using default SHA2_256 */
					SELECT [WorkDB].[dbo].[ufn_genHashCol](DEFAULT,'dbo','DimDate','no_pk')

	Changes:		steveb	20230308	initial version
					steveb	20230309	added multi-column exclude, changing @PK input parameter to @EXCLUDE
										added explicit CAST AS NVARCHAR(MAX) inside COALESCE() to avoid type issues
=============================================================================*/
CREATE OR ALTER FUNCTION [dbo].[ufn_genHashCol]
	(
		@ALGO VARCHAR(8) = 'SHA2_256',
		@SCHEMA NVARCHAR(128),
		@TABLE NVARCHAR(128),
		@EXCLUDE NVARCHAR(MAX)
	)
	RETURNS NVARCHAR(MAX)
AS
BEGIN
	DECLARE @TEMP TABLE([exclude_columm] NVARCHAR(MAX));
	DECLARE @LIST NVARCHAR(MAX) = '';
	DECLARE @OUTPUT NVARCHAR(MAX) = '';
	DECLARE @TARGET NVARCHAR(128) = CONCAT(@SCHEMA,',',@TABLE);

	INSERT @TEMP SELECT TRIM(value) FROM STRING_SPLIT(@EXCLUDE,',');

	SELECT @OUTPUT = CONCAT('HASHBYTES(''',@ALGO,''',(CONCAT(COALESCE(CAST(',STRING_AGG(QUOTENAME(COLUMN_NAME), ' AS NVARCHAR(MAX)),''^''),''|'',COALESCE(CAST(') WITHIN GROUP (ORDER BY ORDINAL_POSITION),' AS NVARCHAR(MAX)),''^''))))')
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_NAME = @TABLE
	AND COLUMN_NAME NOT IN (SELECT [exclude_columm] FROM @TEMP);

	RETURN @OUTPUT
END
GO