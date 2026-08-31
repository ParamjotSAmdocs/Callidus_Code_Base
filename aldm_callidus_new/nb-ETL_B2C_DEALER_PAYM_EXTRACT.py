# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "/Workspace/Repos/Aldm Repository/ALDM/databricks/config/config"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Aldm Repository/ALDM/databricks/transformer/aldm_callidus/nb-Callidus_functions"

# COMMAND ----------

import snowflake.connector
sf_Options_py = {
  "user":f"{sfUser}",
  "private_key":f"{pem_private_key}",
  "account":"THREEMOBILE.west-europe.azure",
  "database":f"{sfDatabase}",
  "warehouse":f"{sfWarehouse}",
  "schema": f"{sfSchema}",
  "disable_ocsp_checks":"True"
}
conn = snowflake.connector.connect(**sf_Options_py)
cs = conn.cursor()

# COMMAND ----------

options = dict(sfUrl = f"{sfUrl}",sfUser = f"{sfUser}",
               pem_private_key = f"{pem_private_key}",sfDatabase = f"{sfDatabase}",
               sfSchema = f"{sf_target_schema}",sfWarehouse = f"{sfWarehouse}")

# COMMAND ----------

period_name_value = dbutils.widgets.get("PERIOD_NAME")
# period_name_value = 'April 2026 M'
print(period_name_value)

# COMMAND ----------

query = f"""

DELETE FROM {sfDatabase}.PRS_CEG_EXT.B2C_DEALER_PAYM_EXTRACT
--WHERE PERIOD_NAME = TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M'
WHERE PERIOD_NAME  = '{period_name_value}'
;
"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

query = f"""

INSERT INTO {sfDatabase}.PRS_CEG_EXT.B2C_DEALER_PAYM_EXTRACT (
    PERIOD_NAME,
    PARTNER_CODE,
    PARTNER_NAME,
    SERVICE_TYPE,
    CONNECTION_ID,
    CONNECTION_DATE,
    DISCONNECTION_DATE,
    DISCONNECTION_REASON,
    ADJUSTMENT_DATE,
    ADDON_DATE,
    IMEI1,
    IMEI2,
    PREPAY_TOPUP_DATE,
    PREPAY_TOPUP_REF,
    EVENT_TYPE,
    ICCID,
    INITIAL_MSISDN,
    PORTIN_MSISDN,
    DEVICE_PRODUCT_CODE,
    DEVICE_PRODUCT_NAME,
    SOURCE,
    TARIFF_ID,
    TARIFF_NAME,
    TENURE_MONTHS,
    ADDON_NAME,
    FIRST_NAME,
    LAST_NAME,
    SOLD_BY_DEALER_CODE,
    COMMENTS,
    CONNECTIONS,
    TOTAL_PAYMENT,
    TARIFF_BONUS,
    DEVICE_BONUS,
    ADDON_BONUS,
    USIM_REFUND,
    ADVANCE_PAYMENT,
    MISCELLANEOUS
)
SELECT
    TAB.PERIOD_NAME,
    TAB.PARTNER_CODE,
    TAB.PARTNER_NAME,
    TAB.BUS_SERVICE_TYPE,
    TAB.CONNECTION_ID,
    TAB.CONNECTION_DT,
    TAB.DISCONNECTION_DT,
    TAB.REASON_CODE,
    TAB.ADJUSTMENT_DATE,
    TAB.ADDON_DATE,
    TAB.IMEI1,
    TAB.IMEI2,
    TAB.PAYG_TOPUP_DT,
    TAB.TXN_REF_NO,
    TAB.EVENT_TYPE,
    TAB.ICCID,
    TAB.INITIAL_MSISDN,
    TAB.PORTIN_MSISDN,
    TAB.PRODUCT_CODE,
    TAB.PRODUCT_DESC,
    'ALDM',
    TAB.TARIFF_CODE,
    TAB.TARIFF_DESC,
    TAB.TENURE,
    TAB.ADDON_NAME,
    TAB.FIRST_NAME,
    TAB.LAST_NAME,
    TAB.SOLD_BY_DEALER_CODE,
    TAB.COMMENTS,
    TAB.CONNECTIONS,
    TAB.TOTAL_PAYMENT,
    NVL(TAB.TARIFF_BONUS, 0)
        + NVL(TAB.MRCSUBSIDY_BONUS, 0)
        + NVL(TAB.MRCSHARE_BONUS, 0),
    TAB.PROMOTION_BONUS,
    NVL(TAB.ADDON_MRCSHARE_BONUS, 0)
        + NVL(TAB.ADDON_MRCSUBS_BONUS, 0)
        + NVL(TAB.ADDON_BONUS, 0),
    TAB.USIM_REFUND,
    TAB.ADVANCE_PAYMENT_BONUS,
    TAB.MISCELLANEOUS
FROM {sfDatabase}.PRS_CEG_EXT.B2C_DEALER_DETAIL_STATEMENT TAB
WHERE TAB.SOURCE NOT IN ('Legacy', 'Business Paid')
  AND TAB.BUS_SERVICE_TYPE = 'PAYM'
  --AND TAB.PERIOD_NAME = TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M'
  AND TAB.PERIOD_NAME  = '{period_name_value}'
  AND TAB.TOTAL_PAYMENT != 0
  AND TAB.IS_POSTED = 0;

"""

#queryobject = sfUtils1.runQuery(options1, query)
cs.execute(query)

# COMMAND ----------

# # import pandas as pd
# query = f""" SELECT
#         PARTNER_CODE                          AS "Partner Code",
#         PARTNER_NAME                          AS "Partner Name",
#         SERVICE_TYPE                          AS "Service Type",
#         CONNECTION_ID                         AS "Connection ID",
#         CONNECTION_DATE                       AS "Connection Date",
#         DISCONNECTION_DATE                    AS "Disconnection Date",
#         DISCONNECTION_REASON                  AS "Disconnection Reason",
#         ADJUSTMENT_DATE                       AS "Adjustment Date",
#         ADDON_DATE                            AS "Add-On Date",
#         IMEI1                                 AS "IMEI1",
#         IMEI2                                 AS "IMEI2",
#         PREPAY_TOPUP_DATE                     AS "PrePay TopUp Date",
#         PREPAY_TOPUP_REF                      AS "PrePay Topup Ref",
#         EVENT_TYPE                            AS "Event Type",
#         ICCID                                 AS "ICCID",
#         INITIAL_MSISDN                        AS "Initial MSISDN",
#         PORTIN_MSISDN                         AS "Port In MSISDN",
#         DEVICE_PRODUCT_CODE                   AS "Device Product Code",
#         DEVICE_PRODUCT_NAME                   AS "Device Product Name",
#         SOURCE                                AS "Source",
#         TARIFF_ID                             AS "Tariff ID",
#         TARIFF_NAME                           AS "Tariff Name",
#         TENURE_MONTHS                         AS "Tenure(Months)",
#         ADDON_NAME                            AS "Add-On Name",
#         FIRST_NAME                            AS "First Name",
#         LAST_NAME                             AS "Last Name",
#         SOLD_BY_DEALER_CODE                   AS "Sold By Dealer Code",
#         COMMENTS                              AS "Comments",
#         CONNECTIONS                           AS "Connections",
#         TOTAL_PAYMENT                         AS "Total Payment",
#         TARIFF_BONUS                          AS "Tariff Bonus",
#         DEVICE_BONUS                          AS "Device Bonus",
#         ADDON_BONUS                           AS "Addon Bonus",
#         USIM_REFUND                           AS "uSIM Refund",
#         ADVANCE_PAYMENT                       AS "Advance Payment",
#         MISCELLANEOUS                         AS "Miscellaneous"
#     FROM {sfDatabase}.PRS_CEG_EXT.B2C_DEALER_PAYM_EXTRACT
#     --WHERE PERIOD_NAME = TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M'
#     WHERE PERIOD_NAME  = '{period_name_value}'
#     """

# COMMAND ----------

# from pyspark.sql.functions import lit
# df_dealer= spark.read \
#   .format("snowflake") \
#   .options(**options) \
#   .option("query", query).load()

# COMMAND ----------

# print(df_dealer)

# COMMAND ----------

import pandas as pd
query = f""" SELECT
        PARTNER_CODE                          AS "Partner Code",
        PARTNER_NAME                          AS "Partner Name",
        SERVICE_TYPE                          AS "Service Type",
        CONNECTION_ID                         AS "Connection ID",
        CONNECTION_DATE                       AS "Connection Date",
        DISCONNECTION_DATE                    AS "Disconnection Date",
        DISCONNECTION_REASON                  AS "Disconnection Reason",
        ADJUSTMENT_DATE                       AS "Adjustment Date",
        ADDON_DATE                            AS "Add-On Date",
        IMEI1                                 AS "IMEI1",
        IMEI2                                 AS "IMEI2",
        PREPAY_TOPUP_DATE                     AS "PrePay TopUp Date",
        PREPAY_TOPUP_REF                      AS "PrePay Topup Ref",
        EVENT_TYPE                            AS "Event Type",
        ICCID                                 AS "ICCID",
        INITIAL_MSISDN                        AS "Initial MSISDN",
        PORTIN_MSISDN                         AS "Port In MSISDN",
        DEVICE_PRODUCT_CODE                   AS "Device Product Code",
        DEVICE_PRODUCT_NAME                   AS "Device Product Name",
        SOURCE                                AS "Source",
        TARIFF_ID                             AS "Tariff ID",
        TARIFF_NAME                           AS "Tariff Name",
        TENURE_MONTHS                         AS "Tenure(Months)",
        ADDON_NAME                            AS "Add-On Name",
        FIRST_NAME                            AS "First Name",
        LAST_NAME                             AS "Last Name",
        SOLD_BY_DEALER_CODE                   AS "Sold By Dealer Code",
        COMMENTS                              AS "Comments",
        CONNECTIONS                           AS "Connections",
        TOTAL_PAYMENT                         AS "Total Payment",
        TARIFF_BONUS                          AS "Tariff Bonus",
        DEVICE_BONUS                          AS "Device Bonus",
        ADDON_BONUS                           AS "Addon Bonus",
        USIM_REFUND                           AS "uSIM Refund",
        ADVANCE_PAYMENT                       AS "Advance Payment",
        MISCELLANEOUS                         AS "Miscellaneous"
    FROM {sfDatabase}.PRS_CEG_EXT.B2C_DEALER_PAYM_EXTRACT
    --WHERE PERIOD_NAME = TO_VARCHAR(DATEADD(MONTH, -1, DATE_TRUNC('MONTH', CURRENT_DATE)), 'MMMM YYYY') || ' M'
    WHERE PERIOD_NAME  = '{period_name_value}'
    """
cs.execute(query)
headers = [col[0] for col in cs.description]
rows = cs.fetchall()
if rows:
    df_data = pd.DataFrame(rows, columns=headers)
    # display(df_data)
else:
    # Provide explicit schema (column names) for empty DataFrame
    df_data = pd.DataFrame(columns=headers)

# COMMAND ----------

# MAGIC %md
# MAGIC exit notebook temporarly

# COMMAND ----------

# dbutils.notebook.exit("Notebook exited.")

# COMMAND ----------

# %sql
# create or replace TABLE ALDM_OPER.EXTRACT_SEQ_TRACK (
# 	EXTRACT_NAME STRING,
# 	EXTRACT_MONTH STRING,
# 	SEQ_NUM INT
# );

# COMMAND ----------

import re

period = re.sub(r'\s*M$', '', period_name_value)         # Remove ' M' at the end if exists
period = re.sub(r'(\w+)\s+(\d{4})', lambda m: m.group(1)[:3].upper() + m.group(2), period)
# print(period)


month_str = period_name_value.split()[0][:3].capitalize()

year_str = period_name_value.split()[1]
# print(month_str, year_str)
month_map = {"Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06","Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"}
month_str = f"{month_map.get(month_str, '00')}{year_str}"
print("period:", period)
print("month_str:", month_str)

# COMMAND ----------

# %pip install snowflake-connector-python pandas PyPDF2

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

# def file_seq_generator1(lv_ext_filename,month_str):
#     from pathlib import Path
#     import datetime

#     ext_filename_pttrn = lv_ext_filename
#     lv_ext_filename_base = Path(lv_ext_filename).stem
#     lv_ext_file_ext = Path(lv_ext_filename).suffix
#     if not month_str:
#         now = datetime.datetime.now()
#         month_str = (now.replace(day=1) - datetime.timedelta(days=1)).strftime("%m%Y")
#     lv_ext_filename_base = lv_ext_filename_base.replace("MMYYYY", month_str)

#     # Query for current sequence number for this file/month
#     select_seq_sql = f"""
#     SELECT SEQ_NUM FROM PRD_ALDM.ALDM_OPER.EXTRACT_SEQ_TRACK
#     WHERE EXTRACT_NAME = '{lv_ext_filename}'
#         AND EXTRACT_MONTH = '{month_str}'
#     """
#     df = spark.sql(select_seq_sql)
#     row = df.first()
#     if row:
#         # If exists, increment sequence and update table
#         seq_num = row[0] + 1
#         update_seq_sql = f"""
#         UPDATE ALDM_OPER.EXTRACT_SEQ_TRACK
#         SET SEQ_NUM = {seq_num}
#         WHERE EXTRACT_NAME = '{lv_ext_filename}'
#             AND EXTRACT_MONTH = '{month_str}'
#         """
#         spark.sql(update_seq_sql)
#     else:
#         # If not exists, insert new row with seq_num = 1
#         seq_num = 1
#         insert_seq_sql = f"""
#         INSERT INTO ALDM_OPER.EXTRACT_SEQ_TRACK (EXTRACT_NAME, EXTRACT_MONTH, SEQ_NUM)
#         VALUES ('{lv_ext_filename}', '{month_str}', {seq_num})
#         """
#         spark.sql(insert_seq_sql)

#     # Format sequence as 4-digit string and build final filename
#     seq_str = f"{seq_num:04d}"
#     lv_ext_filename_base = lv_ext_filename_base.replace('<SEQ>', seq_str)
#     lv_ext_filename_final = f"{lv_ext_filename_base}{lv_ext_file_ext}"
#     # print(lv_ext_filename)
#     # print(lv_ext_filename_final, seq_str)
#     return (lv_ext_filename_final,ext_filename_pttrn,month_str,seq_str)

# COMMAND ----------

# %pip install azure-storage-file-share

from pathlib import Path
import datetime
import os
# from azure.storage.fileshare import ShareFileClient
# from azure.storage.fileshare import ShareDirectoryClient

# Set extract location and filename pattern
lv_extract_location = f"{mft_out}/aldm/outbound_1/B2C_DEALER_DETAILED_STATEMENT_PAYM"
lv_ext_filename = 'DEALER_DETAILED_STATEMENT_PAYM_MMYYYY_<SEQ>.csv'

try:
    # Check if DataFrame has rows to export
    if df_data.shape[0] > 0:
        print("DataFrame has rows. Proceeding with file generation.")
        lv_ext_filename_final, ext_filename_pttrn, month_str, seq_str = file_seq_generator(lv_ext_filename,month_str)
        print("Final Extract Name: ", lv_ext_filename_final)
        print("Extract Filename Pattern: ", ext_filename_pttrn)
        print("Extract Month :", month_str)
        print("Extract Seq :", seq_str)

        # Write DataFrame to CSV
        df_data.to_csv(os.path.join(lv_extract_location, lv_ext_filename_final), index=False, header=True)
        print(f"CSV file '{lv_ext_filename_final}' generated successfully at '{lv_extract_location}'.")

        # Archive old files in Azure File Share
        print("Archiving old files in Azure File Share...")
        archive_files_at_azure_share(fs_conn_str, fs_name, "callidus/B2C_DEALER_DETAILED_STATEMENT_PAYM", "DEALER_DETAILED_STATEMENT_PAYM")
        print("Old files archived successfully.")

        print("Uploading new file to Azure File Share...")
        move_files_to_azure_share(fs_conn_str, fs_name, f"{mft_out}/aldm/outbound_1/B2C_DEALER_DETAILED_STATEMENT_PAYM", "callidus/B2C_DEALER_DETAILED_STATEMENT_PAYM", "DEALER_DETAILED_STATEMENT_PAYM")
        print("New file uploaded successfully.")
    else:
        print("DataFrame is empty. Skipping CSV file generation and Azure upload.")
except Exception as e:
    print(f"Error encountered: {e}")
    raise  # Fail notebook if any error occurs

# COMMAND ----------

# # %pip install azure-storage-file-share

# from pathlib import Path
# import datetime
# import os
# from azure.storage.fileshare import ShareFileClient
# from azure.storage.fileshare import ShareDirectoryClient

# headers = cs.description
# output = cs.fetchall()

# #CHANGE EXTRACT LOCATION & FILENAME 
# lv_extract_location = '/Volumes/dev_aldm/aldm_oper/mft-out/aldm/outbound_1/B2C_DELAER_DETAILED_STATEMENT_PAYM'
# lv_ext_filename = 'DEALER_DETAILED_STATEMENT_PAYM_MMYY_<SEQ>.csv'


# if  output:
#         lv_ext_filename_base = Path(lv_ext_filename).stem
#         lv_ext_file_ext = Path(lv_ext_filename).suffix
#         now = datetime.datetime.now()
#         month_str = now.strftime("%m%y")                                     ### DEPENDS ON THE FORMAT OF THE FILE
#         lv_ext_filename_base = lv_ext_filename_base.replace("MMYY", month_str)

        
#         select_seq_sql = f"""
#         SELECT SEQ_NUM FROM ALDM_OPER.EXTRACT_SEQ_TRACK
#         WHERE EXTRACT_NAME = '{lv_ext_filename}'
#           AND EXTRACT_MONTH = '{month_str}'
#         """
#         df = spark.sql(select_seq_sql)
#         row = df.first()
#         if row:
#             seq_num = row[0] + 1
#             update_seq_sql = f"""
#             UPDATE ALDM_OPER.EXTRACT_SEQ_TRACK
#             SET SEQ_NUM = {seq_num}
#             WHERE EXTRACT_NAME = '{lv_ext_filename}'
#               AND EXTRACT_MONTH = '{month_str}'
#             """
#             spark.sql(update_seq_sql)
#         else:
#             seq_num = 1
#             insert_seq_sql = f"""
#             INSERT INTO ALDM_OPER.EXTRACT_SEQ_TRACK (EXTRACT_NAME, EXTRACT_MONTH, SEQ_NUM)
#             VALUES ('{lv_ext_filename}', '{month_str}', {seq_num})
#             """
#             spark.sql(insert_seq_sql)

#         seq_str = f"{seq_num:04d}"
#         lv_ext_filename_base = lv_ext_filename_base.replace('<SEQ>', seq_str)
#         lv_ext_filename = f"{lv_ext_filename_base}{lv_ext_file_ext}"

#         with open(os.path.join(lv_extract_location, lv_ext_filename), 'w', newline='') as csv_file:
#             csv_writer = csv.writer(csv_file)
#             csv_writer.writerow([col[0] for col in headers])
#             csv_writer.writerows(output)
#         print(f"CSV file '{lv_ext_filename}' generated successfully.")


#         connection_string = "DefaultEndpointsProtocol=https;AccountName=dlsaldmdevweu001;AccountKey=<SET_FROM_KEYVAULT>;EndpointSuffix=core.windows.net"  ########HAVE TO MOVE TO  KEY-VALULT INSTEAD OF HARDCODING 
#         share_name = 'aldm-callidus'      #### TBD SHARE_NAME
#         dir_path = 'callidus/PAYM/'        #### WILL BE DIFF FOR DIFF EXTRACTS

#         dir_client = ShareDirectoryClient.from_connection_string(conn_str=connection_string, share_name=share_name, directory_path=dir_path)

#         for item in dir_client.list_directories_and_files():
#             if not item.is_directory:
                
#                 share_client = ShareFileClient.from_connection_string(conn_str=connection_string,share_name = share_name, file_path=dir_path + 'archive/' + item.name)
#                 share_client.upload_file(item.name)
#                 dir_client.delete_file(item.name)
#                 print(f"File '{item.name}' has been moved to archive folder!!!")


#         share_client = ShareFileClient.from_connection_string(conn_str=connection_string,share_name = share_name, file_path=dir_path + lv_ext_filename)
#         with open(os.path.join(lv_extract_location, lv_ext_filename), "rb") as source_file:
#             share_client.upload_file(source_file)
#         print(f"File '{lv_ext_filename}' has been uploaded to Azure File Share.")
# else: 
#     print("Query returned 0 rows. Skipping CSV file generation.")