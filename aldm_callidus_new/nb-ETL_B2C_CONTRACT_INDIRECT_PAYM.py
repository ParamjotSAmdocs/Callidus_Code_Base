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

DELETE FROM {sfDatabase}.PRS_CEG_EXT.B2C_CONTRACT_INDIRECT_PAYM
--WHERE PERIOD_NAME = TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M'
WHERE PERIOD_NAME = '{period_name_value}'
;
"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

CREATE OR REPLACE TEMP TABLE ATO_ADX_STG.B2C_CONTRACT_TEMP AS 
SELECT
    TLR.PERIOD_NAME,
    TLR.PERIOD_START_DATE,
    TLR.PERIOD_END_DATE,
    TLR.CONNECTION_ID,
    TLR.CONNECTION_DT,
    TLR.DISCONNECTION_DT,
    TLR.LOB,
    TLR.REPORT_DATE,
    TLR.WEEK_IN_YEAR,
    TLR.PARTNER_CODE,
    TLR.PARTNER,
    TLR.MSISDN,
    TLR.TRIPLE_NET,
    TLR.CONTRACT_TERM,
    TLR.DEVICE_PRICEBOOK,
    TLR.DEVICE,
    TLR.TARIFF,
    TLR.TARIFF_PRICE,
    TLR.ADDON_VALUE,
    TLR.DISCOUNT_VALUE,
    TLR.MRC,
    TLR.MRC_LESS_VAT,
    TLR.TARIFF_MRC,
    TLR.ADDON_MRC,
    TLR.TARIFF_BONUS_SUBSIDY,
    TLR.ADDON_BONUS_SUBSIDY,
    TLR.PROMO_BONUS_SUBSIDY,

    CASE
        WHEN TLR.IS_POSTED = '0'
         AND NVL(TLR.CONTRACT_TERM,0) <> 0
        THEN NVL(TLR.TARIFF_BONUS_SUBSIDY,0)
           + NVL(TLR.ADDON_BONUS_SUBSIDY,0)
           + NVL(TLR.PROMO_BONUS_SUBSIDY,0)
    END AS TOTAL_DEVICE_SUBSIDY,
	
    CASE
        WHEN TLR.IS_POSTED = '0'
         AND NVL(TLR.CONTRACT_TERM,0) <> 0
        THEN NVL(TLR.TARIFF_MRC,0)
           + NVL(TLR.ADDON_MRC,0)
    END AS TOTAL_MRC_SHARE,

    CASE
        WHEN TLR.IS_POSTED = '0'
         AND NVL(TLR.CONTRACT_TERM,0) <> 0
        THEN NVL(TLR.TARIFF_BONUS_SUBSIDY,0)
           + NVL(TLR.ADDON_BONUS_SUBSIDY,0)
           + NVL(TLR.PROMO_BONUS_SUBSIDY,0)
           + NVL(TLR.TARIFF_MRC,0)
           + NVL(TLR.ADDON_MRC,0)
    END AS COMMISSION,

    CASE
        WHEN TLR.IS_POSTED = '0'
         AND NVL(TLR.CONTRACT_TERM,0) <> 0
        THEN NVL(TLR.MRC_LESS_VAT,0)
           - ( (
                (
                    NVL(TLR.TARIFF_BONUS_SUBSIDY,0)
                  + NVL(TLR.ADDON_BONUS_SUBSIDY,0)
                  + NVL(TLR.PROMO_BONUS_SUBSIDY,0)
                ) * 1.09
             ) / TLR.CONTRACT_TERM )
    END AS INDIRECT_NMRC,
	
    CASE
        WHEN TLR.IS_POSTED = '0'
         AND NVL(TLR.CONTRACT_TERM,0) <> 0
        THEN
            (
                NVL(TLR.MRC_LESS_VAT,0)
                - ((
                    (
                        NVL(TLR.TARIFF_BONUS_SUBSIDY,0)
                      + NVL(TLR.ADDON_BONUS_SUBSIDY,0)
                      + NVL(TLR.PROMO_BONUS_SUBSIDY,0)
                    ) * 1.09
                  ) / TLR.CONTRACT_TERM )
            )
            -
            ((
                NVL(TLR.TARIFF_MRC,0)
              + NVL(TLR.ADDON_MRC,0)
            ) / TLR.CONTRACT_TERM )
    END AS NMRC_INC_REVSHARE,

    TLR.TARGETED_MRC,
    TLR.PARTNER_MARGIN,
    TLR.DEVICE_COST_PRICE,
    TLR.SOURCE,
    TLR.REVISED_SUBSIDY,
    TLR.REVISED_MRC_SHARE,
    TLR.IS_POSTED,
    TLR.IS_FINALIZED,
    TLR.MODIFIED_DATE,
    TLR.CUSTOMER_TYPE,
    TLR.ACCOUNTING_DATE,
    TLR.ORDER_ID,
    TLR.BAN,
    TLR.PRODUCT_GROUP
FROM
(
    SELECT
        BDD.PERIOD_NAME AS PERIOD_NAME,
        BDD.PERIOD_START_DATE AS PERIOD_START_DATE,
        BDD.PERIOD_END_DATE AS PERIOD_END_DATE,
        BDD.CONNECTION_ID AS CONNECTION_ID,
        BDD.CONNECTION_DT AS CONNECTION_DT,
        BDD.DISCONNECTION_DT AS DISCONNECTION_DT,
        BDD.LOB AS LOB,
        BDD.CONNECTION_DT AS REPORT_DATE,
        BDD.WEEK_IN_YEAR AS WEEK_IN_YEAR,
        'IP_' || BDD.PARTNER_CODE AS PARTNER_CODE,
        BDD.PARTNER_NAME AS PARTNER,
        BDD.INITAL_MSISDN AS MSISDN,
        BDD.TRIPLE_NET AS TRIPLE_NET,
        BDD.TENURE AS CONTRACT_TERM,
        BDD.PRODUCT_CODE AS DEVICE_PRICEBOOK,
        BDD.PRODUCT_DESC AS DEVICE,
        BDD.IND_REP_TARIFF AS TARIFF,
        BDD.IND_REP_TARIFF_PRICE AS TARIFF_PRICE,
        BDD.IND_REP_ADDON_VALUE AS ADDON_VALUE,
        BDD.IND_REP_DISCOUNT_VALUE AS DISCOUNT_VALUE,
        BDD.IND_REP_MRC AS MRC,
        BDD.MRC_LESS_VAT AS MRC_LESS_VAT,

        BDD.TARGETED_MRC AS TARGETED_MRC,
        BDD.PARTNER_MARGIN AS PARTNER_MARGIN,
        BDD.DEVICE_COST_PRICE AS DEVICE_COST_PRICE,

        SUM(0) AS SOURCE,
        SUM(0) AS REVISED_SUBSIDY,
        SUM(0) AS REVISED_MRC_SHARE,

        MAX('0') AS IS_POSTED,
        MAX('0') AS IS_FINALIZED,
        MAX(CURRENT_DATE()) AS MODIFIED_DATE,

        BDD.CUSTOMER_TYPE AS CUSTOMER_TYPE,
        BDD.ACCOUNTING_DATE AS ACCOUNTING_DATE,
        BDD.ORDER_ID AS ORDER_ID,
        BDD.BAN AS BAN,
        BDD.PRODUCT_GROUP AS PRODUCT_GROUP,

        MAX(
            CASE
                WHEN TRIM(UPPER(BDD.CREDIT_TYPE)) NOT LIKE 'CT%ADDON%'
                 AND (
                        TRIM(UPPER(BDD.CREDIT_TYPE)) LIKE 'CT%MRCSHARE%'
                     OR TRIM(UPPER(BDD.CREDIT_TYPE)) LIKE 'CT%TARIFF%'
                     OR TRIM(UPPER(BDD.CREDIT_TYPE)) LIKE 'CT%RECURRING%'
                     OR TRIM(UPPER(BDD.CREDIT_TYPE)) LIKE 'CT%MONTHLY_RENEWAL%'
                 )
                THEN BDD.CREDIT_VAL
            END
        ) AS TARIFF_MRC,

        MAX(
            CASE
                WHEN TRIM(UPPER(BDD.CREDIT_TYPE)) NOT IN (
                    'CT_CVHA_ADDON_SUBSIDY',
                    'CT_CVHU_ADDON_SUBSIDY',
                    'CT_CVHA_ADDON_SUBSIDY_BACK',
                    'CT_CVHU_ADDON_SUBSIDY_BACK',
                    'CT_CMHA_ADDON_SUBSIDY',
                    'CT_CMHU_ADDON_SUBSIDY',
                    'CT_CMHA_ADDON_SUBSIDY_BACK',
                    'CT_CMHU_ADDON_SUBSIDY_BACK',
                    'CT_CHHA_ADDON_SUBSIDY',
                    'CT_CHHU_ADDON_SUBSIDY',
                    'CT_CHHA_ADDON_SUBSIDY_BACK',
                    'CT_CHHU_ADDON_SUBSIDY_BACK',
                    'CT_CMHA_PARTIAL_ADDON_SUBSIDY_BACK',
                    'CT_CMHU_PARTIAL_ADDON_SUBSIDY_BACK',
                    'CT_CHHA_PARTIAL_ADDON_SUBSIDY_BACK',
                    'CT_CHHU_PARTIAL_ADDON_SUBSIDY_BACK'
                )
                AND TRIM(UPPER(BDD.CREDIT_TYPE)) LIKE 'CT%ADDON%'
                THEN BDD.CREDIT_VAL
            END
        ) AS ADDON_MRC,
	
        MAX(
            CASE
                WHEN TRIM(UPPER(BDD.CREDIT_TYPE)) IN (
                    'CT_CVHA_SUBSIDY',
                    'CT_CVHU_SUBSIDY',
                    'CT_CVHA_SUBSIDY_BACK',
                    'CT_CVHU_SUBSIDY_BACK',
                    'CT_CMHA_SUBSIDY',
                    'CT_CMHU_SUBSIDY',
                    'CT_CMHA_SUBSIDY_BACK',
                    'CT_CMHU_SUBSIDY_BACK',
                    'CT_CHHA_SUBSIDY',
                    'CT_CHHU_SUBSIDY',
                    'CT_CHHA_SUBSIDY_BACK',
                    'CT_CHHU_SUBSIDY_BACK',
                    'CT_CMHA_PARTIAL_SUBSIDY_BACK',
                    'CT_CMHU_PARTIAL_SUBSIDY_BACK',
                    'CT_CHHA_PARTIAL_SUBSIDY_BACK',
                    'CT_CHHU_PARTIAL_SUBSIDY_BACK'
                )
                THEN BDD.CREDIT_VAL
            END
        ) AS TARIFF_BONUS_SUBSIDY,

        MAX(
            CASE
                WHEN TRIM(UPPER(BDD.CREDIT_TYPE)) IN (
                    'CT_CVHA_ADDON_SUBSIDY',
                    'CT_CVHU_ADDON_SUBSIDY',
                    'CT_CVHA_ADDON_SUBSIDY_BACK',
                    'CT_CVHU_ADDON_SUBSIDY_BACK',
                    'CT_CMHA_ADDON_SUBSIDY',
                    'CT_CMHU_ADDON_SUBSIDY',
                    'CT_CMHA_ADDON_SUBSIDY_BACK',
                    'CT_CMHU_ADDON_SUBSIDY_BACK',
                    'CT_CHHA_ADDON_SUBSIDY',
                    'CT_CHHU_ADDON_SUBSIDY',
                    'CT_CHHA_ADDON_SUBSIDY_BACK',
                    'CT_CHHU_ADDON_SUBSIDY_BACK',
                    'CT_CMHA_PARTIAL_ADDON_SUBSIDY_BACK',
                    'CT_CMHU_PARTIAL_ADDON_SUBSIDY_BACK',
                    'CT_CHHA_PARTIAL_ADDON_SUBSIDY_BACK',
                    'CT_CHHU_PARTIAL_ADDON_SUBSIDY_BACK'
                )
                THEN BDD.CREDIT_VAL
            END
        ) AS ADDON_BONUS_SUBSIDY,

        MAX(
            CASE
                WHEN TRIM(UPPER(BDD.CREDIT_TYPE)) LIKE 'CT%DEVICE%'
                  OR TRIM(UPPER(BDD.CREDIT_TYPE)) LIKE 'CT%HANDSET%'
                THEN BDD.CREDIT_VAL
            END
        ) AS PROMO_BONUS_SUBSIDY

    FROM {sfDatabase}.PRS_CEG_EXT.B2C_DEALER_DETAILS BDD
    WHERE TRIM(UPPER(BDD.SOURCE)) NOT IN ('LEGACY')
    AND TRIM(UPPER(BDD.BUS_SERVICE_TYPE)) IN ('PAYM')
	--AND BDD.PERIOD_NAME = TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M'
    AND BDD.PERIOD_NAME  = '{period_name_value}'
    GROUP BY ALL
) TLR
ORDER BY
    TLR.PARTNER_CODE,
    TLR.DEVICE
;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfDatabase}.PRS_CEG_EXT.B2C_CONTRACT_INDIRECT_PAYM
SELECT 
	 PERIOD_NAME 
	,PERIOD_START_DATE 
	,PERIOD_END_DATE 
	,CONNECTION_ID 
	,CONNECTION_DT 
	,DISCONNECTION_DT 
	,LOB 
	,REPORT_DATE 
	,WEEK_IN_YEAR 
	,PARTNER_CODE 
	,PARTNER 
	,MSISDN 
	,TRIPLE_NET 
	,CONTRACT_TERM 
	,DEVICE_PRICEBOOK 
	,DEVICE 
	,TARIFF 
	,TARIFF_PRICE 
	,ADDON_VALUE
	,DISCOUNT_VALUE 
	,MRC 
	,MRC_LESS_VAT 
	,TARIFF_MRC 
	,ADDON_MRC 
	,TARIFF_BONUS_SUBSIDY 
	,ADDON_BONUS_SUBSIDY 
	,PROMO_BONUS_SUBSIDY 
	,TOTAL_DEVICE_SUBSIDY 
	,TOTAL_MRC_SHARE 
	,COMMISSION 
	,INDIRECT_NMRC 
	,NMRC_INC_REVSHARE 
	,TARGETED_MRC 
	,PARTNER_MARGIN 
	,DEVICE_COST_PRICE 
	,SOURCE 
	,REVISED_SUBSIDY 
	,REVISED_MRC_SHARE 
	,IS_POSTED 
	,IS_FINALIZED 
	,MODIFIED_DATE 
	,CUSTOMER_TYPE 
	,ACCOUNTING_DATE
	,ORDER_ID 
	,BAN 
	,PRODUCT_GROUP
FROM ATO_ADX_STG.B2C_CONTRACT_TEMP 
WHERE MODIFIED_DATE = CURRENT_DATE()
;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

cs.close()
conn.close()