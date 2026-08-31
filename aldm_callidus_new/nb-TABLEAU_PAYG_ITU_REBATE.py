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

DELETE FROM {sfReportingDatabase}.PRS_SPL_DP.PAYG_ITU_REBATE;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfReportingDatabase}.PRS_SPL_DP.PAYG_ITU_REBATE
(
    ORDERID,
    ORACLE_ORDER_ID,
    PARTNER_NAME,
    PARTNER_CODE,
    COMPENSATIONDATE,
    PRODUCT_CODE,
    PRODUCT_DESC,
    RRP_EX_VAT,
    TRADE_PRICE_EX_VAT,
    REBATE_RATE,
    REBATE_PERCENTAGE,
    ENHANCED_REBATE_FLAG,
    SHIPMENT_QTY,
    GROSS_RRP_INVOICE_VALUE,
    NET_TRADE_PRICE_VALUE,
    REBATE_VALUE,
	PERIODNAME
)
SELECT
    ORDERID,
    ORACLE_ORDER_ID,
    PARTNER_NAME,
    PARTNER_CODE,
    COMPENSATIONDATE,
    PRODUCT_CODE,
    PRODUCT_DESC,
    RRP_EX_VAT,
    (RRP_EX_VAT - REBATE_RATE) AS TRADE_PRICE_EX_VAT,
    REBATE_RATE,
    (REBATE_RATE / NULLIF(RRP_EX_VAT, 0)) * 100 AS REBATE_PERCENTAGE,
    ENHANCED_REBATE_FLAG,
    SHIPMENT_QTY,
    GROSS_RRP_INVOICE_VALUE,
    SHIPMENT_QTY * (RRP_EX_VAT - REBATE_RATE) AS NET_TRADE_PRICE_VALUE,
    REBATE_VALUE,
	PERIODNAME
FROM {sfDatabase}.PRS_CEG_EXT.PAYG_ITU_REBATE
WHERE PERIODNAME IN (
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'YYYY') || ' M',
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -2, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -2, CURRENT_DATE()), 'YYYY') || ' M',
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -3, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -3, CURRENT_DATE()), 'YYYY') || ' M',
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -4, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -4, CURRENT_DATE()), 'YYYY') || ' M',
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -5, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -5, CURRENT_DATE()), 'YYYY') || ' M',
    INITCAP(TRIM(TO_CHAR(DATEADD(MONTH, -6, CURRENT_DATE()), 'MMMM'))) || ' ' || TO_CHAR(DATEADD(MONTH, -6, CURRENT_DATE()), 'YYYY') || ' M'
);




"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

cs.close()
conn.close()