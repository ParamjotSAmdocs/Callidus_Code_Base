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
INSERT INTO {sfDatabase}.ATO_CEG_STG.TBL_PRS_PRICEBOOK_COMMISSIONS_STG
(
	SOURCE_TABLE,
	MDLT_NAME,
	EFFECTIVE_START_DT,
	EFFECTIVE_END_DT,
	VALUE,
	UNIT_TYPE_FOR_VALUE,
	PARTNER_CODE,
	PARTNER_NAME,
	ADDON_CODE,
	TENURE,
	TIER_LEVEL,
	PLANTYPE,
	SMARTY_CHANNEL,
	PRODUCT_CODE,
	PRODUCT_ID,
	REWARD_TYPE,
	REWARD_ELEMENTS,
	MRC,
	LOB,
	TARIFF_CODE,
	COMMITMENT_TYPE,
	SOURCE_ATTR,
	ID,
	INSERT_TS,
	UPDATE_TS
)
SELECT
	SRC.SOURCE_TABLE,
	SRC.MDLT_NAME,
	SRC.EFFECTIVE_START_DT,
	SRC.EFFECTIVE_END_DT,
	SRC.VALUE,
	SRC.UNIT_TYPE_FOR_VALUE,
	SRC.PARTNER_CODE,
	SRC.PARTNER_NAME,
	SRC.ADDON_CODE,
	SRC.TENURE,
	SRC.TIER_LEVEL,
	SRC.PLANTYPE,
	SRC.SMARTY_CHANNEL,
	SRC.PRODUCT_CODE,
	SRC.PRODUCT_ID,
	SRC.REWARD_TYPE,
	SRC.REWARD_ELEMENTS,
	SRC.MRC,
	SRC.LOB,
	SRC.TARIFF_CODE,
	SRC.COMMITMENT_TYPE,
	SRC.SOURCE_ATTR,
	SRC.ID,
	CURRENT_TIMESTAMP() AS INSERT_TS,
	CURRENT_TIMESTAMP() AS UPDATE_TS
FROM {sfCEGDatabase}.PRS_COMMISSIONS.TBL_PRS_PRICEBOOK_COMMISSIONS SRC
;


"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)