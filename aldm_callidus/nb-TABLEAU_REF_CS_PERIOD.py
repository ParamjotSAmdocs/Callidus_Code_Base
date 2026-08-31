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
DELETE FROM {sfReportingDatabase}.PRS_SPL_DP.CS_PERIOD;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------


query = f"""
INSERT INTO {sfReportingDatabase}.PRS_SPL_DP.CS_PERIOD
(
	TENANTID,
	PERIODSEQ,
	CREATEDATE,
	REMOVEDATE,
	CREATEDBY,
	MODIFIEDBY,
	NAME,
	SHORTNAME,
	DESCRIPTION,
	CALENDARSEQ,
	PARENTSEQ,
	PERIODTYPESEQ,
	STARTDATE,
	ENDDATE,
	INSERT_TS,
	UPDATE_TS
)
SELECT
	SRC.TENANTID,
	SRC.PERIODSEQ,
	SRC.CREATEDATE,
	SRC.REMOVEDATE, 
	SRC.CREATEDBY,
	SRC.MODIFIEDBY,
	SRC.NAME,
	SRC.SHORTNAME,
	SRC.DESCRIPTION,
	SRC.CALENDARSEQ,
	SRC.PARENTSEQ,
	SRC.PERIODTYPESEQ,
	SRC.STARTDATE,
	SRC.ENDDATE, 
	SRC.INSERT_TS,
	SRC.UPDATE_TS
FROM {sfDatabase}.ATO_CEG_STG.CS_PERIOD SRC
;


"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)