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

# period_name_value = dbutils.widgets.get("PERIOD_NAME")
# print(period_name_value)

# COMMAND ----------

dbutils.widgets.text("if_email_trigger", "false")
if_email_trigger = dbutils.widgets.get("if_email_trigger")

# COMMAND ----------

# DBTITLE 1,Check for email trigger or bau run
if if_email_trigger == "true":        # Job 1 email_trigger
    files = dbutils.jobs.taskValues.get(
    taskKey="EMAIL_INGEST", key="in_execution", default=[], debugValue= []
    )
    for f in files:
        source_file = f["source_file"]
        period_name_value = f["period"]
        Sequence    = f["sequence"]
    print(period_name_value)
else:                             # Job 2
    # period_name_value = dbutils.jobs.taskValues.get(taskKey="Callidus_Trigger_Check", key="PERIOD_NAME", debugValue="DEBUG")
    period_name_value = dbutils.widgets.get("PERIOD_NAME")
    print(period_name_value) 

# COMMAND ----------

# period_name_value = 'April 2026 M'

# COMMAND ----------

query = f"""

DELETE FROM {sfDatabase}.PRS_CEG_EXT.CALLIDUS_INVOICE_PDF_MONTHLY
--WHERE PERIOD_NAME = TO_VARCHAR(DATEADD(MONTH, -1, CURRENT_DATE),'MMMM YYYY') || ' M'
WHERE PERIOD_NAME = '{period_name_value}'
;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfDatabase}.PRS_CEG_EXT.CALLIDUS_INVOICE_PDF_MONTHLY
SELECT
PARTY.LASTNAME														AS PARTICIPANT_NAME
,PARTY.GENERICATTRIBUTE1											AS PARTICIPANT_ADDRESS_LINE1
,PARTY.GENERICATTRIBUTE2											AS PARTICIPANT_ADDRESS_LINE2
,PARTY.GENERICATTRIBUTE3											AS PARTICIPANT_ADDRESS_LINE3
,PARTY.GENERICATTRIBUTE4											AS PARTICIPANT_ADDRESS_LINE4
,PARTY.GENERICATTRIBUTE5											AS PARTICIPANT_ADDRESS_LINE5
,DEP.PERIOD															AS PERIOD_NAME
,PER.STARTDATE::DATE												AS PERIOD_START_DATE
,DATEADD(DAY,-1,PER.ENDDATE::DATE) 									AS PERIOD_END_DATE
,PARTY.GENERICATTRIBUTE12 || TO_CHAR(CURRENT_DATE, 'DDMMYY')		AS INVOICE_NO
,CURRENT_DATE														AS INVOICE_DATE
,PARTY.GENERICATTRIBUTE8											AS YOUR_REF
,PARTY.GENERICATTRIBUTE13											AS CONTACT
,PARTY.GENERICATTRIBUTE9											AS ACCOUNT_PAYABLE_REF
,'IP_' || PARTY.GENERICATTRIBUTE12									AS ACCOUNT_NO
,PARTY.GENERICATTRIBUTE10											AS PAYMENT_TERMS
,PARTY.GENERICATTRIBUTE6											AS SUPPLIER_VAT_REGISTRATION_NO
,DEP.GA1_LINE_TYPE_DESC												AS EARNING_GROUP
,SUM(NVL(DEP.DEPOSIT_AMOUNT,0))										AS VALUE
,NVL(PARTY.GENERICNUMBER1,0) * 100									AS VAT_RATE

FROM (
	SELECT *
	FROM (
		SELECT * FROM ATO_CEG_STG.TBL_FACT_PAYM_DEPOSITS
		UNION
		SELECT * FROM ATO_CEG_STG.TBL_FACT_PAYG_DEPOSITS
	)
	--WHERE PERIOD = TO_VARCHAR(DATEADD(MONTH, -1, CURRENT_DATE),'MMMM YYYY') || ' M'
	WHERE PERIOD = '{period_name_value}'

) DEP

INNER JOIN (
	SELECT * FROM ATO_CEG_STG.CS_GENERICCLASSIFIER C
	QUALIFY ROW_NUMBER() OVER(PARTITION BY C.GENERICATTRIBUTE1 ORDER BY C.REMOVEDATE DESC) = 1
) C
ON DEP.EARNING_GROUPS = C.GENERICATTRIBUTE1

INNER JOIN (
	SELECT * FROM ATO_CEG_STG.CS_CATEGORY_CLASSIFIERS B
	QUALIFY ROW_NUMBER() OVER(PARTITION BY B.CLASSIFIERSEQ ORDER BY B.REMOVEDATE DESC) = 1
) B
ON C.CLASSIFIERSEQ = B.CLASSIFIERSEQ

INNER JOIN (
	SELECT * FROM ATO_CEG_STG.CS_CATEGORYTREE A
	WHERE UPPER(TRIM(A.NAME)) = 'GL STRINGS'
	QUALIFY ROW_NUMBER() OVER(PARTITION BY A.CATEGORYTREESEQ ORDER BY A.REMOVEDATE DESC) = 1
) A
ON B.CATEGORYTREESEQ = A.CATEGORYTREESEQ

INNER JOIN (
	SELECT * FROM ATO_CEG_STG.CS_CLASSIFIER D
	QUALIFY ROW_NUMBER() OVER(PARTITION BY D.CLASSIFIERSEQ ORDER BY D.REMOVEDATE DESC) = 1
) D
ON B.CLASSIFIERSEQ = D.CLASSIFIERSEQ

LEFT JOIN (
	SELECT * FROM ATO_CEG_STG.CS_PARTICIPANT
	QUALIFY ROW_NUMBER() OVER(PARTITION BY GENERICATTRIBUTE12 ORDER BY REMOVEDATE DESC) = 1
) PARTY
ON DEP.PARTNER_CODE = 'IP_' || PARTY.GENERICATTRIBUTE12

LEFT JOIN (
	SELECT * FROM ATO_CEG_STG.CS_PERIOD PER
	QUALIFY ROW_NUMBER() OVER(PARTITION BY PER.NAME ORDER BY PER.REMOVEDATE DESC) = 1
) PER
ON DEP.PERIOD = PER.NAME

GROUP BY
PARTICIPANT_NAME
,PARTICIPANT_ADDRESS_LINE1
,PARTICIPANT_ADDRESS_LINE2
,PARTICIPANT_ADDRESS_LINE3
,PARTICIPANT_ADDRESS_LINE4
,PARTICIPANT_ADDRESS_LINE5
,PERIOD_NAME
,PERIOD_START_DATE
,PERIOD_END_DATE
,INVOICE_NO
,INVOICE_DATE
,YOUR_REF
,CONTACT
,ACCOUNT_PAYABLE_REF
,ACCOUNT_NO
,PAYMENT_TERMS
,SUPPLIER_VAT_REGISTRATION_NO
,EARNING_GROUP
,VAT_RATE
;


"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

cs.close()
conn.close()