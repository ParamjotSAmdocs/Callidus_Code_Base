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
# debug_check = [
#     {
#         "source_file": "20260806103538_confirmation_email.json",
#         "period": "April 2026 M",
#         "sequence": "0000",
#     }
# ]
files = dbutils.jobs.taskValues.get(
    taskKey="EMAIL_INGEST", key="in_execution", default=[], debugValue=[]
)
for f in files:
    source_file = f["source_file"]
    period_name_value = f["period"]
    sequence    = f["sequence"]
 

# COMMAND ----------

query = f"""

-- DEP
CREATE OR REPLACE TABLE {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYM_PAYG_DEPOSITS AS
SELECT *
FROM {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYM_DEPOSITS
--WHERE PERIOD = TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M'
WHERE PERIOD = '{period_name_value}'

UNION

SELECT *
FROM {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYG_DEPOSITS
--WHERE PERIOD = TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M'
WHERE PERIOD = '{period_name_value}';
"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

-- CRED
CREATE OR REPLACE TABLE {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYM_PAYG_CREDITS AS
SELECT *
FROM {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYM_CREDITS

UNION

SELECT *
FROM {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYG_CREDITS;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

-- SALESTRANS
CREATE OR REPLACE TABLE {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYM_PAYG_SALES_ACTIVITY AS
SELECT *
FROM {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYM_SALES_ACTIVITY

UNION

SELECT *
FROM {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYG_SALES_ACTIVITY;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

--IMP NOTE: INCASE OF TECHNICAL RERUN UNCOMMENT THIS DELETE STATEMENT THEN ONLY RERUN

--DELETE FROM {sfDatabase}.ATO_CEG_STG.CALLIDUS_OUT_COMM_CREDITS
--WHERE PERIODNAME = '{period_name_value}';

"""

#queryobject = sfUtils1.runQuery(options1, query)
# cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfDatabase}.ATO_CEG_STG.CALLIDUS_OUT_COMM_CREDITS
(
    PERIODNAME,
    PERIOD_START_DATE,
    PERIOD_END_DATE,
    PAYMENT_RUN_NO,
    PARTICIPANT_ID,
    ACCOUNT_PAY_REF,
    ACCOUNT_RECV_REF,
    PARTNER_NAME,
    VAT_LOCATION,
    VAT_NO,
    DOCNO,
    DOC_DATE,
    PAYMENT_DOC_TYPE,
    PAYDOCLINETYPE,
    PAYDOCLINETYPEDESC,
    ORDERID,
    LINENUMBER,
    SUBLINENUMBER,
    EVENTTYPE,
    COMPENSATIONDATE,
    CREDITTYPEID,
    CREDIT_VALUE,
    PAYMENT_TYPE
)
SELECT
    TLR.PERIODNAME,
    TLR.PERIOD_START_DATE,
    TLR.PERIOD_END_DATE,
    RUN.PAYMENT_RUN_NO,
    TLR.PARTICIPANT_ID,
    TLR.ACCOUNT_PAY_REF,
    TLR.ACCOUNT_RECV_REF,
    TLR.PARTNER_NAME,
    TLR.VAT_LOCATION,
    TLR.VAT_NO,
    TLR.DOCNO,
    TLR.DOC_DATE,
    TLR.PAYMENT_DOC_TYPE,
    TLR.PAYDOCLINETYPE,
    TLR.PAYDOCLINETYPEDESC,
    TLR.ORDERID,
    TLR.LINENUMBER,
    TLR.SUBLINENUMBER,
    TLR.EVENTTYPE,
    TLR.COMPENSATIONDATE,
    TLR.CREDITTYPEID,
    TLR.CREDIT_VALUE,
    TLR.PAYMENT_TYPE
FROM
(
    SELECT
        DEP.PERIOD AS PERIODNAME,
        CP.STARTDATE AS PERIOD_START_DATE,
        DATEADD(DAY, -1, CP.ENDDATE) AS PERIOD_END_DATE,

        PARTY.GENERICATTRIBUTE12 AS PARTICIPANT_ID,
        PARTY.GENERICATTRIBUTE9 AS ACCOUNT_PAY_REF,
        PARTY.GENERICATTRIBUTE11 AS ACCOUNT_RECV_REF,
        PARTY.LASTNAME AS PARTNER_NAME,
        PARTY.GENERICATTRIBUTE7 AS VAT_LOCATION,
        PARTY.GENERICATTRIBUTE6 AS VAT_NO,
        PARTY.GENERICATTRIBUTE12 || TO_VARCHAR(CURRENT_DATE, 'DDMMYY') AS DOCNO,
		
        CURRENT_DATE AS DOC_DATE,

        CASE
            WHEN CRED.CREDIT_AMOUNT >= 0
                THEN 'Self bill Invoice'
            ELSE 'Credit Note'
        END AS PAYMENT_DOC_TYPE,

        DEP.EARNING_GROUPS AS PAYDOCLINETYPE,
        DEP.GA1_LINE_TYPE_DESC AS PAYDOCLINETYPEDESC,

        SALESTRANS.ORDER_ID AS ORDERID,
        SALESTRANS.LINE_NUMBER AS LINENUMBER,
        SALESTRANS.SUBLINE_NUMBER AS SUBLINENUMBER,
        SALESTRANS.EVENT_TYPE_ID AS EVENTTYPE,
        SALESTRANS.COMPENSATION_DATE AS COMPENSATIONDATE,

        CRED.CREDIT_TYPE AS CREDITTYPEID,
        CRED.CREDIT_AMOUNT AS CREDIT_VALUE,

        CASE
            WHEN SALESTRANS.EVENT_TYPE_ID LIKE '%Back'
                THEN 'CLAWBACK'
            ELSE 'PAYMENT'
        END AS PAYMENT_TYPE

	FROM {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYM_PAYG_DEPOSITS DEP

    INNER JOIN
    (
        SELECT *
        FROM {sfDatabase}.ATO_CEG_STG.CS_GENERICCLASSIFIER
        QUALIFY ROW_NUMBER() OVER(PARTITION BY GENERICATTRIBUTE3 ORDER BY REMOVEDATE DESC) = 1
    ) C
        ON DEP.EARNING_GROUPS = C.GENERICATTRIBUTE3

    INNER JOIN
    (
        SELECT *
        FROM {sfDatabase}.ATO_CEG_STG.CS_CATEGORY_CLASSIFIERS
        QUALIFY ROW_NUMBER() OVER(PARTITION BY CLASSIFIERSEQ ORDER BY REMOVEDATE DESC) = 1
    ) B
        ON C.CLASSIFIERSEQ = B.CLASSIFIERSEQ

    INNER JOIN
    (
        SELECT *
        FROM {sfDatabase}.ATO_CEG_STG.CS_CATEGORYTREE
        WHERE UPPER(TRIM(NAME)) = 'CREDIT MAPPINGS'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY CATEGORYTREESEQ ORDER BY REMOVEDATE DESC) = 1
    ) A
        ON B.CATEGORYTREESEQ = A.CATEGORYTREESEQ

    INNER JOIN
    (
        SELECT *
        FROM {sfDatabase}.ATO_CEG_STG.CS_CLASSIFIER
        QUALIFY ROW_NUMBER() OVER(PARTITION BY CLASSIFIERSEQ ORDER BY REMOVEDATE DESC) = 1
    ) D
        ON B.CLASSIFIERSEQ = D.CLASSIFIERSEQ

    INNER JOIN {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYM_PAYG_CREDITS CRED
        ON D.NAME = CRED.CREDIT_TYPE
		
	INNER JOIN
	(
		SELECT * FROM {sfDatabase}.ATO_CEG_STG.CS_PERIOD
		QUALIFY ROW_NUMBER() OVER(PARTITION BY NAME ORDER BY REMOVEDATE DESC) = 1
	) CP
	ON DEP.PERIOD = CP.NAME

    INNER JOIN 
	(
		SELECT * FROM {sfDatabase}.ATO_CEG_STG.TBL_FACT_PAYM_PAYG_SALES_ACTIVITY
	) SALESTRANS
        ON CRED.ORDER_ID = SALESTRANS.ORDER_ID
       AND CRED.LINE = SALESTRANS.LINE_NUMBER
       AND CRED.SUBLINE = SALESTRANS.SUBLINE_NUMBER
       AND CRED.EVENT_TYPE = SALESTRANS.EVENT_TYPE_ID
       AND CRED.PERIOD = SALESTRANS.PERIOD

    INNER JOIN
    (
        SELECT *
        FROM {sfDatabase}.ATO_CEG_STG.CS_PARTICIPANT
        QUALIFY ROW_NUMBER() OVER(PARTITION BY GENERICATTRIBUTE12 ORDER BY REMOVEDATE DESC) = 1
    ) PARTY
        ON CRED.PARTNER_CODE = 'IP_' || PARTY.GENERICATTRIBUTE12

  WHERE NVL(UPPER(TRIM(CRED.GA1_EVENT_DESCRIPTION)), 'X') <> 'VOLUME CNT'
	  AND UPPER(TRIM(CRED.CREDIT_TYPE)) NOT IN ('CT_PAYG_ITU_EXTRA','CT_PAYG_ITU','CT_PAYG_NONITU_TOPUP1_AMOUNT')
      AND UPPER(TRIM(SALESTRANS.DATA_SOURCE)) NOT IN ('LEGACY','BUSINESS PAID')
      AND UPPER(TRIM(SALESTRANS.COMPENSATION_DATE)) BETWEEN CP.STARTDATE AND CP.ENDDATE
) TLR

CROSS JOIN
(
    SELECT
        LPAD(
            TO_VARCHAR(
                COALESCE(
                    (
                        SELECT MAX(TRY_TO_NUMBER(PAYMENT_RUN_NO))
                        FROM {sfDatabase}.ATO_CEG_STG.CALLIDUS_OUT_COMM_CREDITS
                    ),
                    0     -- replace '0' with PAYMENT_RUN_NO from CEG for first load OR Insert the number given by CEG for the first time in DEV_IDW.PRS_CEG_EXT.CALLIDUS_OUT_COMM_CREDITS 
                ) + 1
            ),
            6,
            '0'
        ) AS PAYMENT_RUN_NO
) RUN;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

cs.close()
conn.close()