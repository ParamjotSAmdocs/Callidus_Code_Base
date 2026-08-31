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
INSERT INTO {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYM_DEPOSITS_STG
(
	PARTNER_CODE,
	PERIOD,
	DEPOSIT_GENERATED_DATE,
	DEPOSIT_NAME,
	RULE_TYPE,
	DESCRIPTION,
	DEPOSIT_OUTPUT_NAME,
	DEPOSIT_AMOUNT,
	EARNING_CODES,
	EARNING_GROUPS,
	DISPLAY_NAME_FOR_REPORTS,
	GA1_LINE_TYPE_DESC,
	INSERT_TS,
	UPDATE_TS
)
SELECT
	SRC.PARTNER_CODE,
	SRC.PERIOD,
	SRC.DEPOSIT_GENERATED_DATE,
	SRC.DEPOSIT_NAME,
	SRC.RULE_TYPE,
	SRC.DESCRIPTION,
	SRC.DEPOSIT_OUTPUT_NAME,
	SRC.DEPOSIT_AMOUNT,
	SRC.EARNING_CODES,
	SRC.EARNING_GROUPS,
	SRC.DISPLAY_NAME_FOR_REPORTS,
	SRC.GA1_LINE_TYPE_DESC,
	CURRENT_TIMESTAMP() AS INSERT_TS,
	CURRENT_TIMESTAMP() AS UPDATE_TS
FROM {sfCEGDatabase}.PRS_COMMISSIONS.TBL_FACT_PAYM_DEPOSITS SRC
;


"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)