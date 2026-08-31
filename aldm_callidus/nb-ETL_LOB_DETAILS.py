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

period_name_value = dbutils.widgets.get("PERIOD_NAME")
print(period_name_value)

# COMMAND ----------

query = f"""

CREATE OR REPLACE TABLE {sfDatabase}.ATO_CEG_STG.LOB_DETAILS_TEMP AS
SELECT
A.TENANTID AS TENANTID,
D.CLASSIFIERID AS CLASSIFIERID,
D.NAME AS CLASSIFIERNAME,
C.GENERICATTRIBUTE1 AS EVENT_TYPE,
C.GENERICATTRIBUTE2 AS EARNING_CODE,
C.GENERICATTRIBUTE3 AS EARNING_GROUP,
C.GENERICATTRIBUTE4 AS LOB,
C.EFFECTIVESTARTDATE AS EFFECTIVESTARTDATE,
C.EFFECTIVEENDDATE AS EFFECTIVEENDDATE,
CURRENT_TIMESTAMP() AS MODIFIED_DATE,
C.GENERICATTRIBUTE5 AS SERVICETYPE,
C.GENERICATTRIBUTE6 AS BUSINESSTYPE,
C.GENERICATTRIBUTE7 AS BUSINESS_EVENTTYPE

FROM
(
    SELECT CATEGORYTREESEQ, TENANTID, NAME, REMOVEDATE
    FROM {sfDatabase}.ATO_CEG_STG.CS_CATEGORYTREE
    WHERE UPPER(TRIM(NAME)) = 'CREDIT MAPPINGS'
    and removedate = '2200-01-01'
) A
JOIN
(
    SELECT CLASSIFIERSEQ, CATEGORYTREESEQ, REMOVEDATE
    FROM {sfDatabase}.ATO_CEG_STG.CS_CATEGORY_CLASSIFIERS
    where removedate = '2200-01-01'
    ) B
ON A.CATEGORYTREESEQ = B.CATEGORYTREESEQ

JOIN 
(
	SELECT * 
	FROM {sfDatabase}.ATO_CEG_STG.CS_GENERICCLASSIFIER
	where removedate = '2200-01-01'
) C
ON B.CLASSIFIERSEQ = C.CLASSIFIERSEQ

JOIN
(	SELECT CLASSIFIERID, NAME, CLASSIFIERSEQ, REMOVEDATE
	FROM {sfDatabase}.ATO_CEG_STG.CS_CLASSIFIER
	where removedate = '2200-01-01'
) D
ON B.CLASSIFIERSEQ = D.CLASSIFIERSEQ;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

DELETE FROM {sfDatabase}.ATO_CEG_STG.LOB_DETAILS_STG
WHERE CAST(INSERT_TS AS DATE) = CURRENT_DATE;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfDatabase}.ATO_CEG_STG.LOB_DETAILS_STG
SELECT
	 TENANTID
	,CLASSIFIERID
	,CLASSIFIERNAME
	,EVENT_TYPE
	,EARNING_CODE
	,EARNING_GROUP
	,LOB
	,EFFECTIVESTARTDATE
	,EFFECTIVEENDDATE
	,MODIFIED_DATE
	,SERVICETYPE
	,BUSINESSTYPE
	,BUSINESS_EVENTTYPE
	,CURRENT_TIMESTAMP AS INSERT_TS
FROM {sfDatabase}.ATO_CEG_STG.LOB_DETAILS_TEMP;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

DELETE FROM {sfDatabase}.PRS_CEG_EXT.LOB_DETAILS;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfDatabase}.PRS_CEG_EXT.LOB_DETAILS
SELECT
     TENANTID
    ,CLASSIFIERID
    ,CLASSIFIERNAME
    ,EVENT_TYPE
    ,EARNING_CODE
    ,EARNING_GROUP
    ,LOB
    ,EFFECTIVESTARTDATE
    ,EFFECTIVEENDDATE
    ,MODIFIED_DATE
    ,SERVICETYPE
    ,BUSINESSTYPE
    ,BUSINESS_EVENTTYPE
FROM {sfDatabase}.ATO_CEG_STG.LOB_DETAILS_STG
WHERE CAST(INSERT_TS AS DATE) = CURRENT_DATE;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

DELETE FROM {sfDatabase}.ATO_CEG_STG.LOB_DETAILS_STG
WHERE CAST(INSERT_TS AS DATE) < DATEADD(MONTH, -5, CURRENT_DATE);

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)