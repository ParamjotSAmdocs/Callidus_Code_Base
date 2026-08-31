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

# period_name_value = dbutils.jobs.taskValues.get(taskKey="Callidus_Trigger_Check", key="PERIOD_NAME", debugValue="DEBUG")
period_name_value = 'APR26D'
print(period_name_value)

# COMMAND ----------

from pyspark.sql.functions import lit
df_INV= spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("query", f"SELECT DISTINCT INVOICE_NO, ACCOUNT_NO FROM {sfDatabase}.PRS_CEG_EXT.CALLIDUS_INVOICE_PDF_MIDMONTHLY WHERE PERIOD_NAME = '{period_name_value}'").load()


df_inv_status = df_INV.withColumn("STATUS", lit("N"))
display(df_inv_status)

# COMMAND ----------

if df_inv_status.count() == 0:
    dbutils.notebook.exit(f"No records were found for the Mid Monthly Invoice for the {period_name_value} period ")

# COMMAND ----------

if period_name_value and len(period_name_value) == 6:
    month_abbr = period_name_value[:3].upper()
    year_two = period_name_value[3:5]
    week = period_name_value[5].upper()
    period = f"{month_abbr}{'20'+year_two}"
    month_map = {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06",
                 "JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
    mm = month_map.get(month_abbr, "00")
    month_str = mm + "20" + year_two
else:
    period = None
    month_str = None
    week = None

print("period:", period)
print("month_str:", month_str)
print("week:", week)

# COMMAND ----------

from pyspark.sql.functions import when
from datetime import datetime, timedelta

# Get current and previous period strings
# period2 = datetime.now().strftime("%b%Y").upper()
# period = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%b%Y").upper()
# print(period, period2)

# Collect invoice numbers from DataFrame
inv_acc_list = [(row["INVOICE_NO"], row["ACCOUNT_NO"]) for row in df_inv_status.collect()]
print(inv_acc_list)
status_updates = []


remove_files_by_extension(f"{mft_out}/aldm/outbound_1/CALLIDUS/INVOICE", ".pdf")

INVOICE_TYPE = "MID_MONTHLY"

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

# Run notebook for each invoice and track status
for invoice, account_no in inv_acc_list:
    try:
        result = dbutils.notebook.run(
            "/Workspace/Repos/Aldm Repository/ALDM/databricks/transformer/aldm_callidus/nb-Invoice_Generic",
            timeout_seconds=600,
            arguments={
                "invoice_no": str(invoice),
                "extract_filename_format": f"{INVOICE_TYPE}_INVOICE_PDF_{account_no}_{period}{week}_{seq_str}.pdf",
                "fs_dir_path": "callidus/INVOICE/",
                "interface_home": "CALLIDUS/INVOICE",
                "invoice_type": INVOICE_TYPE,
                "period" :period_name_value
            }
        )
        if result is None:
            status_updates.append((invoice, 'Y'))  # Success
        elif result.strip().lower() != "success":
            raise RuntimeError(f"Notebook run failed for invoice {invoice}: {result}")
        else:
            status_updates.append((invoice, 'Y'))  # Success
        print(f"{INVOICE_TYPE}_INVOICE_PDF_{account_no}_{period}{week}_{seq_str}.pdf")
    except Exception as e:
        print(f"Notebook run failed for invoice {invoice}: {e}")
        status_updates.append((invoice, 'N'))  # Failure
        raise

from pyspark.sql import Row

# Update status DataFrame and display
try:
    status_df = spark.createDataFrame([Row(INVOICE_NO=inv, STATUS=stat) for inv, stat in status_updates])
    df_inv_status = df_inv_status.drop("STATUS").join(status_df, on="INVOICE_NO", how="left")
    display(df_inv_status)
except Exception as e:
    print(f"Error updating invoice status DataFrame: {e}")
    raise

# Merge PDFs, archive, and move files to Azure File Share
try:
    merge_and_archive_pdfs_mft("CALLIDUS/INVOICE", "CALLIDUS/INVOICE/FINAL", f"{INVOICE_TYPE}_INVOICE_PDF_{period}{week}_{seq_str}.pdf",INVOICE_TYPE)
    archive_files_at_azure_share(fs_conn_str, fs_name, "callidus/INVOICE", INVOICE_TYPE)
    move_files_to_azure_share(fs_conn_str, fs_name, f"{mft_out}/aldm/outbound_1/CALLIDUS/INVOICE/INDIVIDUAL", "callidus/INVOICE", INVOICE_TYPE)
    move_files_to_azure_share(fs_conn_str, fs_name, f"{mft_out}/aldm/outbound_1/CALLIDUS/INVOICE/FINAL", "callidus/INVOICE", INVOICE_TYPE)
    upd_Invoice_type_latest_trigger(INVOICE_TYPE, seq_str, month_str)
except Exception as e:
    print(f"Error in merging or moving files: {e}")
    raise