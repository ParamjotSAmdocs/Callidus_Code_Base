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

DELETE FROM {sfReportingDatabase}.PRS_SPL_DP.INDIRECT_COMMISSION_EXCEPTION;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfReportingDatabase}.PRS_SPL_DP.INDIRECT_COMMISSION_EXCEPTION
SELECT
ORDER_ID
,LINE_NUMBER
,SUBLINE_NUMBER
,BAN
,INITIAL_MSISDN
,PARTNER_CODE
,PARTNER_NAME
,TARIFF_CODE
,TARIFF_DESC
,DEVICE_ID
,DEVICE_DESC
,ACCOUNTING_DATE
,DISCONNECT_DATE
,DISCONNECT_REASON
,ICCID
,IMEI1
,IMEI2
,PRICE_MODIFIER
,PAYM_HANDSET_PARTNER_CODE AS PAYM_PARTNER_CODE_MISSING_IN_HANDSET_PB
,PAYM_TARIFF_MRC_PARTNER_CODE AS PAYM_PARTNER_CODE_MISSING_IN_TARIFF_MRC_PB
,PAYM_TARIFF_CODE AS PAYM_TARIFF_CODE_MISSING_IN_PB
,PAYM_PRODUCT_CODE AS PAYM_PRODUCT_CODE_MISSING_IN_PB
,PAYM_TENURE AS PAYM_TENURE_MISSING_IN_PB
,PAYG_PARTNER_CODE AS PAYG_PARTNER_CODE_MISSING_IN_PB
,PAYG_PRODUCT_CODE AS PAYG_PRODUCT_CODE_MISSING_IN_PB
,PERIOD
FROM {sfDatabase}.PRS_CEG_EXT.INDIRECT_COMMISSION_EXCEPTION;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

cs.close()
conn.close()