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

DELETE FROM {sfReportingDatabase}.PRS_SPL_DP.CS_TRANSACTIONADDRESS;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------


query = f"""

INSERT INTO {sfReportingDatabase}.PRS_SPL_DP.CS_TRANSACTIONADDRESS
(
	TENANTID,
	TRANSACTIONADDRESSSEQ,
	SALESTRANSACTIONSEQ,
	PROCESSINGUNITSEQ,
	COMPENSATIONDATE,
	ADDRESSTYPESEQ,
	CUSTID,
	CONTACT,
	COMPANY,
	AREACODE,
	PHONE,
	FAX,
	ADDRESS1,
	ADDRESS2,
	ADDRESS3,
	CITY,
	STATE,
	COUNTRY,
	POSTALCODE,
	INDUSTRY,
	GEOGRAPHY,
	INSERT_TS,
	UPDATE_TS
)
SELECT
	SRC.TENANTID,
	SRC.TRANSACTIONADDRESSSEQ,
	SRC.SALESTRANSACTIONSEQ,
	SRC.PROCESSINGUNITSEQ,
	SRC.COMPENSATIONDATE,
	SRC.ADDRESSTYPESEQ,
	SRC.CUSTID,
	SRC.CONTACT,
	SRC.COMPANY,
	SRC.AREACODE,
	SRC.PHONE,
	SRC.FAX,
	SRC.ADDRESS1,
	SRC.ADDRESS2,
	SRC.ADDRESS3,
	SRC.CITY,
	SRC.STATE,
	SRC.COUNTRY,
	SRC.POSTALCODE,
	SRC.INDUSTRY,
	SRC.GEOGRAPHY,
	SRC.INSERT_TS,
	SRC.UPDATE_TS
FROM {sfDatabase}.ATO_CEG_STG.CS_TRANSACTIONADDRESS SRC
;


"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)