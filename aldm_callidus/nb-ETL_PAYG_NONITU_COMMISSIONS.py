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

#period_name_value = dbutils.widgets.get("PERIOD_NAME")
#period_name_value = 'April 2026 M'
#print(period_name_value)

# COMMAND ----------

# period_name_value = dbutils.jobs.taskValues.get(taskKey="Callidus_Trigger_Check", key="PERIOD_NAME", debugValue="DEBUG")
# print(period_name_value)

# COMMAND ----------

query = f"""

DELETE FROM {sfDatabase}.PRS_CEG_EXT.PAYG_NONITU_COMMISSION
--WHERE PERIOD = INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'MMMM')))
--           || ' ' ||
--           TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'YYYY')
--           || ' M'
WHERE PERIOD = '{period_name_value}'
;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfDatabase}.PRS_CEG_EXT.PAYG_NONITU_COMMISSION
(
    PERIOD,
    PARTNER_ID,
    PARTNER_NAME,
    PAYMENT_TYPE,
    COMMISSION_AMOUNT,
	 INSERT_TS,
    UPDATE_TS
)

SELECT

   PD.PERIOD as  PERIOD,
    
   
    PD.PARTNER_CODE AS PARTNER_ID,
    
  
    CP.LASTNAME AS PARTNER_NAME,
 
    SUBSTR(PD.DEPOSIT_NAME, LENGTH('DR_MCAL_IP_') + 1) AS PAYMENT_TYPE,
    
    -- COMMISSION_AMOUNT: sum of DEPOSIT_AMOUNT
    SUM(PD.DEPOSIT_AMOUNT) AS COMMISSION_AMOUNT,
	CURRENT_TIMESTAMP() AS INSERT_TS,
    CURRENT_TIMESTAMP() AS UPDATE_TS

FROM {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYG_DEPOSITS PD

LEFT JOIN (
    SELECT 
        GENERICATTRIBUTE12,
        LASTNAME,
        REMOVEDATE
    FROM {sfDatabase}.ATO_CEG_STG.CS_PARTICIPANT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY GENERICATTRIBUTE12
        ORDER BY  -- K_EOT first
            REMOVEDATE DESC
    ) = 1
) CP
    ON    'IP_' || CP.GENERICATTRIBUTE12  = PD.PARTNER_CODE
--WHERE PD.PERIOD = INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'MMMM')))
--           || ' ' ||
--           TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'YYYY')
--           || ' M'
WHERE PD.PERIOD = '{period_name_value}'
AND UPPER(PD.EARNING_CODES) IN (
        'EC_IP_PAYG_REBATE','EC_IP_PAYG_VOLUME','EC_PAYG_1ST_TOPUP','EC_PAYG_REVSHARE'
      )
  AND UPPER(PD.DEPOSIT_NAME) IN (
         'DR_MCAL_IP_PAYG_NONITU_REBATE',
    'DR_MCAL_IP_PAYG_NONITU_REVSHARE',
    'DR_MCAL_IP_PAYG_NONITU_TOPUP1',
    'DR_MCAL_IP_PAYG_NONITU_VOLUME_BONUS_CLAWBACK',
    'DR_MCAL_IP_PAYG_NONITU_VOLUME_BONUS_CLAWBACK_ANN_TRUEUP'
      )

GROUP BY 
     PD.PERIOD,
    PD.PARTNER_CODE,
    CP.LASTNAME,
    SUBSTR(PD.DEPOSIT_NAME, LENGTH('DR_MCAL_IP_') + 1);


"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

cs.close()
conn.close()