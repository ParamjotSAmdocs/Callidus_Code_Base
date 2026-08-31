# Databricks notebook source
dbutils.notebook.exit("Notebook exited, as Invoice Monthly pdf report is not generated from Tableau, So Tableau Table refresh is skipped.")

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Aldm Repository/ALDM/databricks/config/config"

# COMMAND ----------

--------import snowflake.connector
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

DELETE FROM {sfReportingDatabase}.PRS_SPL_DP.CALLIDUS_INVOICE_PDF_MONTHLY;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfReportingDatabase}.PRS_SPL_DP.CALLIDUS_INVOICE_PDF_MONTHLY
SELECT
*
FROM {sfDatabase}.PRS_CEG_EXT.CALLIDUS_INVOICE_PDF_MONTHLY;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

cs.close()
conn.close()