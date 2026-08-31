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

DELETE FROM {sfReportingDatabase}.PRS_SPL_DP.PAYG_NONITU_COMMISSION;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfReportingDatabase}.PRS_SPL_DP.PAYG_NONITU_COMMISSION
SELECT
*
FROM {sfDatabase}.PRS_CEG_EXT.PAYG_NONITU_COMMISSION
WHERE PERIOD IN (
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