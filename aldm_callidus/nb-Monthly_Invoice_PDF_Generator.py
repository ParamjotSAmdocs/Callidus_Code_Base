# Databricks notebook source
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

dbutils.notebook.exit("Notebook execution completed successfully")

# COMMAND ----------

from pyspark.sql.functions import lit
df_INV= spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("query", f"select distinct invoice_no, account_no from {sfDatabase}.PRS_CEG_EXT.CALLIDUS_INVOICE_PDF_MONTHLY where PERIOD_NAME = '{period_name_value}' ").load()

  # .option("query",  """select distinct invoice_no, account_no from DEV_IDW.PRS_CEG_EXT.CALLIDUS_INVOICE_PDF_MONTHLY """).load()
df_inv_status_monthly = df_INV.withColumn("STATUS", lit("N"))
display(df_inv_status_monthly)

# COMMAND ----------

if df_inv_status_monthly.count() == 0:
    dbutils.notebook.exit(f"No records were found for the Monthly Invoice for the {period_name_value} period ")

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

from pyspark.sql.functions import when
from datetime import datetime, timedelta

# Get current and previous period strings
# period2 = datetime.now().strftime("%b%Y").upper()
# period = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%b%Y").upper()
# print(period, period2)

# Collect invoice numbers from DataFrame
inv_acc_list = [(row["INVOICE_NO"], row["ACCOUNT_NO"]) for row in df_inv_status_monthly.collect()]
print(inv_acc_list)
status_updates = []

remove_files_by_extension("/Volumes/sit_aldm/aldm_oper/mft-out/aldm/outbound_1/POC/INVOICE", ".pdf")

INVOICE_TYPE = "MONTHLY"

# Generate extract filename and sequence details
try:
    lv_ext_filename_final, ext_filename_pttrn, month_str, seq_str = file_seq_generator("INVOICE",month_str)
    print("Final Extract Name: ", lv_ext_filename_final)
    print("Extract Filename Pattern: ", ext_filename_pttrn)
    print("Extract Month :", month_str)
    print("Extract Seq :", seq_str)
except Exception as e:
    print(f"Error generating file sequence: {e}")
    raise

# Run Snowflake invoice PDF notebook for each invoice and track status
for invoice, account_no in inv_acc_list:
    try:
        result = dbutils.notebook.run(
            "/Workspace/Repos/Aldm Repository/ALDM/databricks/transformer/aldm_callidus/nb-Invoice_Generic",
            timeout_seconds=600,
            arguments={
                "invoice_no": str(invoice),
                "extract_filename_format": f"{INVOICE_TYPE}_INVOICE_PDF_{account_no}_{period}_{seq_str}.pdf",
                "fs_dir_path": "callidus/INVOICE/",
                "interface_home": "POC/INVOICE",
                "invoice_type": INVOICE_TYPE,
                "period": period_name_value
            }
        )
        if result is None:
            status_updates.append((invoice, 'Y'))  # Success
        elif result.strip().lower() != "success":
            raise RuntimeError(f"Notebook run failed for invoice {invoice}: {result}")
        else:
            status_updates.append((invoice, 'Y'))  # Success
        print(f"{INVOICE_TYPE}_INVOICE_PDF_{account_no}_{period}_{seq_str}.pdf")
    except Exception as e:
        print(f"Notebook run failed for invoice {invoice}: {e}")
        status_updates.append((invoice, 'N'))  # Failure
        raise

from pyspark.sql import Row

# Update status DataFrame and display
try:
    status_df = spark.createDataFrame([Row(INVOICE_NO=inv, STATUS=stat) for inv, stat in status_updates])
    df_inv_status_monthly = df_inv_status_monthly.drop("STATUS").join(status_df, on="INVOICE_NO", how="left")
    display(df_inv_status_monthly)
except Exception as e:
    print(f"Error updating invoice status DataFrame: {e}")
    raise

# Merge PDFs, archive, and move files to Azure File Share
try:
    merge_and_archive_pdfs_mft("POC/INVOICE", "POC/INVOICE/FINAL", f"{INVOICE_TYPE}_INVOICE_PDF_{period}_{seq_str}.pdf", INVOICE_TYPE)
    archive_files_at_azure_share(fs_conn_str, fs_name, "callidus/INVOICE", INVOICE_TYPE)
    move_files_to_azure_share(fs_conn_str, fs_name, f"{mft_out}/aldm/outbound_1/POC/INVOICE/INDIVIDUAL", "callidus/INVOICE", INVOICE_TYPE)
    move_files_to_azure_share(fs_conn_str, fs_name, f"{mft_out}/aldm/outbound_1/POC/INVOICE/FINAL", "callidus/INVOICE", INVOICE_TYPE)
    upd_Invoice_type_latest_trigger(INVOICE_TYPE, seq_str, month_str)
except Exception as e:
    print(f"Error in merging or moving files: {e}")
    raise