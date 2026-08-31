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
TRUNCATE TABLE {sfDatabase}.ATO_CEG_STG.CS_CLASSIFIER;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------


query = f"""
INSERT INTO {sfDatabase}.ATO_CEG_STG.CS_CLASSIFIER
(
	TENANTID,
	CLASSIFIERSEQ,
	EFFECTIVESTARTDATE,
	EFFECTIVEENDDATE,
	ISLAST,
	CREATEDATE,
	REMOVEDATE,
	CREATEDBY,
	MODIFIEDBY,
	CLASSIFIERID,
	NAME,
	SELECTORID,
	BUSINESSUNITMAP,
	DESCRIPTION,
	INSERT_TS,
	UPDATE_TS
)
SELECT
	SRC.TENANTID,
	SRC.CLASSIFIERSEQ,
	SRC.EFFECTIVESTARTDATE,
	SRC.EFFECTIVEENDDATE,
	SRC.ISLAST,
	SRC.CREATEDATE,
	SRC.REMOVEDATE,
	SRC.CREATEDBY,
	SRC.MODIFIEDBY,
	SRC.CLASSIFIERID,
	SRC.NAME,
	SRC.SELECTORID,
	SRC.BUSINESSUNITMAP,
	SRC.DESCRIPTION,
	SRC.INSERT_TS,
	SRC.UPDATE_TS
FROM {sfDatabase}.ATO_CEG_STG.CS_CLASSIFIER_STG SRC
WHERE SRC.INSERT_TS = (
	SELECT MAX(INSERT_TS)
	FROM {sfDatabase}.ATO_CEG_STG.CS_CLASSIFIER_STG
)
;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)