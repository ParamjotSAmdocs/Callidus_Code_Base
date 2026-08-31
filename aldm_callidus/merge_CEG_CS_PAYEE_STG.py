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
INSERT INTO {sfDatabase}.ATO_CEG_STG.CS_PAYEE_STG
(
	TENANTID,
	PAYEESEQ,
	EFFECTIVESTARTDATE,
	EFFECTIVEENDDATE,
	ISLAST,
	CREATEDATE,
	REMOVEDATE,
	CREATEDBY,
	MODIFIEDBY,
	PAYEEID,
	BUSINESSUNITMAP,
	INSERT_TS,
	UPDATE_TS
)
SELECT
	SRC.TENANTID,
	SRC.PAYEESEQ,
	SRC.EFFECTIVESTARTDATE,
	SRC.EFFECTIVEENDDATE,
	SRC.ISLAST,
	SRC.CREATEDATE,
	SRC.REMOVEDATE,
	SRC.CREATEDBY,
	SRC.MODIFIEDBY,
	SRC.PAYEEID,
	SRC.BUSINESSUNITMAP,
	CURRENT_TIMESTAMP() AS INSERT_TS,
	CURRENT_TIMESTAMP() AS UPDATE_TS
FROM {sfCEGDatabase}.PRS_COMMISSIONS.CS_PAYEE SRC
;


"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)