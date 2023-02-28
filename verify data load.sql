SELECT count(1) FROM intake.SRC_EDS.t_GL_Activity with (nolock)  --10,105,277 dev


select count(1) from intake.[CLEAN].[t_EDS_GL_Activity_History] with (nolock)   --30,857,416 dev   41,857,475 on prod
--select count(1) from intake.CLEAN.t_EDS_GL_Activity with (nolock)  --14,700,243 dev (says it loaded correctly?)     13,698,313 prod

select count(1) from processed.core.t_SAP_Actuals_FieldEng_History with (nolock) --11,605,900 dev  26,786,480 prod  --------------static
select count(1) from processed.core.t_SAP_Actuals --29,100,000 dev   42,147,432 prod-----------------



--select count(1) from intake.[CLEAN].[t_MGB_DTL_CSE_Total_POR_Report] with (nolock)    --533585
--select count(1) from intake.[CLEAN].[t_NN_CapLeaseMovements] with (nolock)   --36
--select count(1) from intake.[CLEAN].[t_EDS_CapitalLeaseMovement_FieldEng] with (nolock)    --273090
--select count(1) from intake.[CLEAN].[t_FIELDENG_MonthlyAccrual] with (nolock)    --167840

--FORGOT?  select count(1) from processed.core.t_MGB_POR with (nolock)  --533585 dev   533585 prod   ?---------------


--DELETE FROM intake.SRC_EDS.t_GL_Activity with (nolock)  WHERE [BUDAT]>=20220101
--DELETE FROM intake.[CLEAN].[t_EDS_GL_Activity_History] with (nolock)  WHERE PostingDate>='20220101' --30,857,416 dev   41,857,475 on prod
--DELETE FROM processed.core.t_SAP_Actuals where PostingDate>='20220101'

SELECT *  FROM intake.SRC_EDS.t_GL_Activity with (nolock)  WHERE [BUDAT]>=20220101
SELECT *  FROM intake.[CLEAN].[t_EDS_GL_Activity_History] with (nolock)  WHERE PostingDate>='20220101' --30,857,416 dev   41,857,475 on prod
--SELECT TOP 10 PostingDate from processed.core.t_SAP_Actuals_FieldEng_History with (nolock)  WHERE PostingDate>='20220101' 
SELECT *  FROM processed.core.t_SAP_Actuals where PostingDate>='20220101'