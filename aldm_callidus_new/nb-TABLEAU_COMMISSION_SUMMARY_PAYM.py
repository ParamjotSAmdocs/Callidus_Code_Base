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

DELETE FROM {sfReportingDatabase}.PRS_SPL_DP.COMMISSION_SUMMARY_PAYM;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfReportingDatabase}.PRS_SPL_DP.COMMISSION_SUMMARY_PAYM
SELECT
BUS_SERVICE_TYPE,
LOB,
PAYMENT_MONTH,
TRANSACTION_MONTH,
PARTNER_CODE,
PARTNER_NAME,
VOLUME,
TOTAL_PAYMENT,
VB_FLAG
FROM {sfDatabase}.PRS_CEG_EXT.COMMISSION_SUMMARY_PAYM
WHERE RIGHT(TRIM(TRANSACTION_MONTH),1) = 'M'
  AND TO_DATE(
        LEFT(SPLIT_PART(TRANSACTION_MONTH,' ',1),3) 
        || ' ' ||
        SPLIT_PART(TRANSACTION_MONTH,' ',2),
        'MON YYYY'
      )
      BETWEEN
          DATE_TRUNC(
              'YEAR',
              DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
          )
      AND DATE_TRUNC(
              'MONTH',
              DATEADD(MONTH, -1, CURRENT_DATE()) 
          )
;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

cs.close()
conn.close()