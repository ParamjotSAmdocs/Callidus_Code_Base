# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Aldm Repository/ALDM/databricks/config/config"

# COMMAND ----------

import snowflake.connector
sf_Options_py = {
  "user":f"{sfUser}",
  "private_key":f"{pem_private_key}",
  "account":"THREEMOBILE.west-europe.azure",
  "database":f"{sfDatabase}",
  "warehouse":f"{sfWarehouse}",
  "schema": f"{sfNondoxTgtSchema}",
  "disable_ocsp_checks":"True"
}
conn = snowflake.connector.connect(**sf_Options_py)
cs = conn.cursor()

# COMMAND ----------

period_name_value = dbutils.widgets.get("PERIOD_NAME")
print(period_name_value)

# COMMAND ----------

query = f"""

-------------- IN CASE OF RERUN -------------
DELETE FROM {sfDatabase}.PRS_CEG_EXT.COMMISSION_SUMMARY_PAYG
WHERE RIGHT(TRIM(TRANSACTION_MONTH),1) = 'M'
  AND --TO_DATE(
      --  LEFT(SPLIT_PART(TRANSACTION_MONTH,' ',1),3)
      --  || ' ' ||
      --  SPLIT_PART(TRANSACTION_MONTH,' ',2),
      --  'MON YYYY'
      --)
      TO_DATE(REGEXP_REPLACE(TRIM(TRANSACTION_MONTH), ' M$', ''), 'MMMM YYYY')
			BETWEEN
				DATE_TRUNC(
					'YEAR',
					--DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
          TO_DATE(REGEXP_REPLACE('{period_name_value}', ' M$', ''), 'MMMM YYYY') --considering period from trigger
				)
			AND
    --DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
    TO_DATE(REGEXP_REPLACE('{period_name_value}', ' M$', ''), 'MMMM YYYY') --considering period from trigger
          ;
          
"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""
		  
INSERT INTO {sfDatabase}.PRS_CEG_EXT.COMMISSION_SUMMARY_PAYG
(
PERIOD_START_DATE,
TRANSACTION_MONTH,
PAYMENT_MONTH,
BUS_SERVICE_TYPE,
LOB,
PARTNER_CODE,
PARTNER_NAME,
VOLUME,
COMMISSION_TYPE,
TARIFF_BONUS,
PROMOTION_BONUS,
MRC_SHARE_BONUS,
ADVANCE_PAYMENT_BONUS,
USIM_REFUND,
MISC_BONUS,
VOLUME_BONUS,
TACTICAL_BONUS,
ADDON_BONUS,
MRCSUBSIDY_BONUS,
ADDON_MRCSHARE_BONUS,
ADDON_MRCSUBS_BONUS,
TOTAL_PAYMENT,
VB_FLAG
)
-- DATASET 1 : B2C_DEALER_DETAIL_STATEMENT  
SELECT
    STMT.PERIOD_START_DATE    AS PERIOD_START_DATE,
    STMT.PERIOD_NAME          AS TRANSACTION_MONTH,
	TO_CHAR(DATEADD(MONTH, 1, PERIOD_END_DATE), 'MMMM YYYY') AS PAYMENT_MONTH,
    'PAYG'   AS BUS_SERVICE_TYPE,
    CASE
        WHEN STMT.DISCONNECTION_DT IS NULL THEN
            CASE
                WHEN STMT.LOB IS NOT NULL THEN
                    STMT.LOB || ' - ' ||
                        CASE
                            WHEN STMT.EVENT_TYPE = 'Addon' THEN   
                                CASE 
									WHEN SUBSTR(STMT.LOB, -1) = 'A' THEN 'Acquisition'
									WHEN SUBSTR(STMT.LOB, -1) = 'U' THEN 'Upgrade'
									ELSE 'Addon'
								END
                            ELSE STMT.EVENT_TYPE
                        END
                ELSE
                    STMT.EVENT_TYPE
            END
        WHEN STMT.DISCONNECTION_DT IS NOT NULL AND STMT.EVENT_TYPE = 'Trueup' THEN
			CASE 
				WHEN STMT.LOB IS NOT NULL THEN
					STMT.LOB || ' - ' || STMT.EVENT_TYPE
				ELSE STMT.EVENT_TYPE
			END
        ELSE
            STMT.REASON_CODE
    END  AS LOB, 
    STMT.PARTNER_CODE    AS PARTNER_CODE,
    STMT.PARTNER_NAME    AS PARTNER_NAME,
    COUNT(*)             AS VOLUME,
    NULL                 AS COMMISSION_TYPE,
    SUM(STMT.TARIFF_BONUS)              AS TARIFF_BONUS,
    SUM(STMT.PROMOTION_BONUS)           AS PROMOTION_BONUS,
    SUM(STMT.MRCSHARE_BONUS)            AS MRC_SHARE_BONUS,
    SUM(STMT.ADVANCE_PAYMENT_BONUS)     AS ADVANCE_PAYMENT_BONUS,
    SUM(STMT.USIM_REFUND)               AS USIM_REFUND,
    SUM(STMT.MISCELLANEOUS)             AS MISC_BONUS,
    SUM(STMT.VOLUME_BONUS)              AS VOLUME_BONUS,
    SUM(STMT.TACTICAL_BONUS)            AS TACTICAL_BONUS,
    SUM(STMT.ADDON_BONUS)               AS ADDON_BONUS,
    SUM(STMT.MRCSUBSIDY_BONUS)          AS MRCSUBSIDY_BONUS,
    SUM(STMT.ADDON_MRCSHARE_BONUS)      AS ADDON_MRCSHARE_BONUS,
    SUM(STMT.ADDON_MRCSUBS_BONUS)       AS ADDON_MRCSUBS_BONUS,
    SUM(STMT.TOTAL_PAYMENT)             AS TOTAL_PAYMENT,
    NULL                                AS VB_FLAG
FROM {sfDatabase}.PRS_CEG_EXT.B2C_DEALER_DETAIL_STATEMENT STMT
INNER JOIN (
    SELECT *
    FROM {sfDatabase}.ATO_CEG_STG.CS_PERIOD
    QUALIFY ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY REMOVEDATE DESC) = 1
) CP ON STMT.PERIOD_NAME = CP.NAME
WHERE STMT.SOURCE NOT IN ('Legacy', 'Business Paid')  
  AND STMT.BUS_SERVICE_TYPE = 'PAYG' 
  AND	RIGHT(TRIM(PERIOD_NAME),1) = 'M'  --ONLY CONSIDER  PERIODS WHICH HAVE 'M' AT LAST
    -- DELTA 
	 and 
			--  TO_DATE(
			--		LEFT(SPLIT_PART(PERIOD_NAME,' ',1),3)
			--		|| ' ' ||
			--		SPLIT_PART(PERIOD_NAME,' ',2),
			--		'MON YYYY'
			--	)
   			TO_DATE(REGEXP_REPLACE(TRIM(PERIOD_NAME), ' M$', ''), 'MMMM YYYY')
			BETWEEN
				DATE_TRUNC(
					'YEAR',
					--DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
                    TO_DATE(REGEXP_REPLACE('{period_name_value}', ' M$', ''), 'MMMM YYYY') --considering period from trigger
				)
			AND
    --DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
    TO_DATE(REGEXP_REPLACE('{period_name_value}', ' M$', ''), 'MMMM YYYY') --considering period from trigger
	
GROUP BY
    1,2,3,4,5,6,7
UNION ALL


  -- DATASET 2 : TBL_FACT_PAYG_DEPOSITS (VOLUME BONUS ADJUSTMENT)

SELECT
    CP.STARTDATE     AS PERIOD_START_DATE,   
    DEP.PERIOD     AS TRANSACTION_MONTH,  
	TO_CHAR(DATEADD(MONTH, 1, CP.ENDDATE), 'MMMM YYYY') AS PAYMENT_MONTH,
    'PAYG'   AS BUS_SERVICE_TYPE,
    DEP.GA1_LINE_TYPE_DESC     AS LOB,
    CONCAT('IP_', PAR.GENERICATTRIBUTE12)           AS PARTNER_CODE,
    PAR.LASTNAME               AS PARTNER_NAME,
    NULL                       AS VOLUME,
    NULL                       AS COMMISSION_TYPE,
    NULL                       AS TARIFF_BONUS,
    NULL                       AS PROMOTION_BONUS,
    NULL                       AS MRC_SHARE_BONUS,
    NULL                       AS ADVANCE_PAYMENT_BONUS,
    NULL                       AS USIM_REFUND,
    NULL                       AS MISC_BONUS,
    DEP.DEPOSIT_AMOUNT         AS VOLUME_BONUS,
    NULL                       AS TACTICAL_BONUS,
    NULL                       AS ADDON_BONUS,
    NULL                       AS MRCSUBSIDY_BONUS,
    NULL                       AS ADDON_MRCSHARE_BONUS,
    NULL                       AS ADDON_MRCSUBS_BONUS,
    DEP.DEPOSIT_AMOUNT         AS TOTAL_PAYMENT,
    'Volume Bonus'             AS VB_FLAG
FROM {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYG_DEPOSITS DEP    
INNER JOIN (
	SELECT * FROM {sfDatabase}.ATO_CEG_STG.CS_PARTICIPANT
	QUALIFY ROW_NUMBER() OVER(PARTITION BY GENERICATTRIBUTE12 ORDER BY REMOVEDATE DESC) = 1
) PAR
ON DEP.PARTNER_CODE = 'IP_' || PAR.GENERICATTRIBUTE12
INNER JOIN {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYG_INCENTIVES INC
    ON DEP.PARTNER_CODE = INC.PARTNER_CODE
INNER JOIN  (
    SELECT *
    FROM {sfDatabase}.ATO_CEG_STG.CS_PERIOD
    QUALIFY ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY REMOVEDATE DESC) = 1
) CP ON DEP.PERIOD = CP.NAME
WHERE 
   UPPER(INC.INCENTIVE_OUTPUT_NAME) LIKE '%VOLUME_ADJUSTMENT_BP%' 
  AND INC.INCENTIVE_AMOUNT <> 0
  and RIGHT(TRIM(DEP.PERIOD),1) = 'M' --ONLY CONSIDER  PERIODS WHICH HAVE 'M' AT LAST
	-- Delta 
		 and 
			  --TO_DATE(
			--		LEFT(SPLIT_PART(DEP.PERIOD,' ',1),3)
			--		|| ' ' ||
			--		SPLIT_PART(DEP.PERIOD,' ',2),
			--		'MON YYYY'
			--	)
            TO_DATE(REGEXP_REPLACE(TRIM(DEP.PERIOD), ' M$', ''), 'MMMM YYYY')
			BETWEEN
				DATE_TRUNC(
					'YEAR',
					--DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
                    TO_DATE(REGEXP_REPLACE('{period_name_value}', ' M$', ''), 'MMMM YYYY') --considering period from trigger
				)
			AND
    --DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
    TO_DATE(REGEXP_REPLACE('{period_name_value}', ' M$', ''), 'MMMM YYYY') --considering period from trigger
;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)