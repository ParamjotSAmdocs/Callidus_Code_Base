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

DELETE FROM {sfReportingDatabase}.PRS_SPL_DP.PROPOSED_COMMISSION;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfReportingDatabase}.PRS_SPL_DP.PROPOSED_COMMISSION
SELECT
    PARTICIPANTID  AS PARTNER_ID,
    PAYEENAME     AS PARTNER,
    CURRENT_MONTH   AS PROPOSED_COMMISSION,
    MONTH_1    AS COMMISSION_MONTH_1,
    (CURRENT_MONTH - MONTH_1)   AS DEVIATION_MONTH_1,

    CASE
        WHEN CURRENT_MONTH = 0 THEN 9999.99
        ELSE (DEVIATION_MONTH_1/ CURRENT_MONTH) * 100
    END  AS DEVIATION_PCT_MONTH_1,

    MONTH_2   AS COMMISSION_MONTH_2,
    (CURRENT_MONTH - MONTH_2)  AS DEVIATION_MONTH_2,

    CASE
        WHEN CURRENT_MONTH = 0 THEN 9999.99
        ELSE (DEVIATION_MONTH_2/ CURRENT_MONTH) * 100
    END  AS DEVIATION_PCT_MONTH_2,

    MONTH_3  AS COMMISSION_MONTH_3,
    (CURRENT_MONTH - MONTH_3)  AS DEVIATION_MONTH_3,

    CASE
        WHEN CURRENT_MONTH = 0 THEN 9999.99
        ELSE (DEVIATION_MONTH_3/ CURRENT_MONTH) * 100
    END  AS DEVIATION_PCT_MONTH_3,

    YTD  AS COMMISSIONS_YTD,
    NULL  AS COMMISSION_TEAM_COMMENT,
    INV_CNT AS INVOICE_COUNT,
    PERIODNAME AS PERIODNAME

FROM {sfDatabase}.PRS_CEG_EXT.PROPOSED_COMMISSION
WHERE PERIODNAME IN (
    TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M',
    TO_VARCHAR(DATEADD(MONTH, -2, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M',
    TO_VARCHAR(DATEADD(MONTH, -3, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M',
    TO_VARCHAR(DATEADD(MONTH, -4, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M',
    TO_VARCHAR(DATEADD(MONTH, -5, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M',
    TO_VARCHAR(DATEADD(MONTH, -6, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M'

);

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

cs.close()
conn.close()