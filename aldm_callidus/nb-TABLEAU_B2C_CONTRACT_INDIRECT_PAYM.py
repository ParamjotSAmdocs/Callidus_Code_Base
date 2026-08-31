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

DELETE FROM {sfReportingDatabase}.PRS_SPL_DP.B2C_CONTRACT_INDIRECT_PAYM;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfReportingDatabase}.PRS_SPL_DP.B2C_CONTRACT_INDIRECT_PAYM
SELECT
PERIOD_NAME
,PERIOD_START_DATE
,PERIOD_END_DATE
,LOB
,CONNECTION_DT
,WEEK_IN_YEAR
,PARTNER_CODE
,PARTNER
,CONNECTION_ID
,BAN
,MSISDN
,CONTRACT_TERM
,PRODUCT_GROUP
,DEVICE
,TARIFF
,TARIFF_PRICE
,ADDON_VALUE
,MRC
,MRC_LESS_VAT
,TARIFF_MRC
,ADDON_MRC
,TARIFF_BONUS_SUBSIDY
,ADDON_BONUS_SUBSIDY
,PROMO_BONUS_SUBSIDY
,TOTAL_DEVICE_SUBSIDY
,TOTAL_MRC_SHARE
,COMMISSION
,INDIRECT_NMRC
,NMRC_INC_REVSHARE
FROM {sfDatabase}.PRS_CEG_EXT.B2C_CONTRACT_INDIRECT_PAYM
WHERE PERIOD_NAME IN (
    TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M',
    TO_VARCHAR(DATEADD(MONTH, -2, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M',
    TO_VARCHAR(DATEADD(MONTH, -3, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M',
    TO_VARCHAR(DATEADD(MONTH, -4, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M',
    TO_VARCHAR(DATEADD(MONTH, -5, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M',
    TO_VARCHAR(DATEADD(MONTH, -6, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M'

);
"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

cs.close()
conn.close()