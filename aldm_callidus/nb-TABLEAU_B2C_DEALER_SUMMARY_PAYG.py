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

query = f"""

DELETE FROM {sfReportingDatabase}.PRS_SPL_DP.DEALER_SUMMARY_REPORT_PAYG;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfReportingDatabase}.PRS_SPL_DP.DEALER_SUMMARY_REPORT_PAYG
SELECT

PERIOD_NAME             AS      PERIOD_NAME
,PARTNER_NAME            AS     Partner_Name      
,PARTNER_CODE            AS     Partner_Code      
,COMMITMENT_TYPE        AS        COMMITMENT_TYPE
,PERIOD_START_DATE       AS     Start_Date      
,PERIOD_END_DATE         AS     End_Date        
,TARIFF_DESC             AS     Tariff_Description 
,PRODUCT_DESC            AS     Product_Descriptio
,Quantity                AS     Quantity              
,SUBTOTAL_UNIT           AS     Sub_Total_Unit   
,SUBTOTAL_TOTAL          AS     Sub_Total_Total 
,TARIFF_BONUS_UNIT       AS     Tariff_Bonus_Unit      
,TARIFF_BONUS_TOTAL     AS     Tariff_Bonus_Total     
,PROMOTION_BONUS_UNIT    AS     Device_Bonus_Unit      
,PROMOTION_BONUS_TOTAL   AS     Device_Bonus_Total     
,ADDON_BONUS_UNIT        AS     Addon_Bonus_Unit       
,ADDON_BONUS_TOTAL       AS     Addon_Bonus_Total      
,USIM_REFUND_UNIT        AS     USIM_Refund_Unit       
,USIM_REFUND_TOTAL       AS     USIM_Refund_Total      
,ADVANCE_PAY_BONUS_UNIT  AS     Advance_Payment_Unit      
,ADVANCE_PAY_BONUS_TOTAL AS     Advance_Payment_Total     
,MANUAL_ADJ_UNIT         AS     Other_Unit              
,MANUAL_ADJ_BONUS_TOTAL  AS     Other_Total         
,PAYG_TOPUP_BONUS_UNIT   AS     Topup_Bonus_Unit    
,PAYG_TOPUP_BONUS_TOTAL  AS     Topup_Bonus_Total   
,PAYG_REVSHARE_UNIT      AS     Revshare_Unit        
,PAYG_REVSHARE_TOTAL     AS     Revshare_Total      
,PAYG_REBATE_UNIT        AS     Rebate_Unit            
,PAYG_REBATE_TOTAL       AS     Rebate_Total   

FROM {sfDatabase}.PRS_CEG_EXT.B2C_DEALER_SUMMARY
WHERE TRIM(UPPER(BUS_SERVICE_TYPE)) ='PAYG' AND IS_POSTED = 0
AND PERIOD_NAME IN (
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'YYYY') || ' M',
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -2, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -2, CURRENT_DATE()), 'YYYY') || ' M',
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -3, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -3, CURRENT_DATE()), 'YYYY') || ' M',
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -4, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -4, CURRENT_DATE()), 'YYYY') || ' M',
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -5, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -5, CURRENT_DATE()), 'YYYY') || ' M',
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -6, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -6, CURRENT_DATE()), 'YYYY') || ' M'
);

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

cs.close()
conn.close()