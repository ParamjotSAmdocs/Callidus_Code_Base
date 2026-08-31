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

DELETE FROM {sfReportingDatabase}.PRS_SPL_DP.PAYG_TOPUP_COHORT;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfReportingDatabase}.PRS_SPL_DP.PAYG_TOPUP_COHORT
SELECT
*
FROM {sfDatabase}.PRS_CEG_EXT.PAYG_TOPUP_COHORT
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