# Databricks notebook source
# DBTITLE 1,Get Connection Details
# MAGIC %run "/Workspace/Repos/Aldm Repository/ALDM/databricks/config/config"

# COMMAND ----------

# DBTITLE 1,Call Functions
# MAGIC %run "/Workspace/Repos/Aldm Repository/ALDM/databricks/transformer/aldm_callidus/nb-Callidus_functions"

# COMMAND ----------

# DBTITLE 1,Set Job Variables
run_monthly = dbutils.jobs.taskValues.get(
    taskKey="EMAIL_INGEST", key="run_monthly", default="false", debugValue="false"
)
run_mid_monthly = dbutils.jobs.taskValues.get(
    taskKey="EMAIL_INGEST", key="run_mid_monthly", default="false", debugValue="false"
)

debug_check = [
    {
        "source_file": "20260806103538_confirmation_email.json",
        "period": "April 2026 M",
        "sequence": "0005",
    }
]
files = dbutils.jobs.taskValues.get(
    taskKey="EMAIL_INGEST", key="in_execution", default=[], debugValue= debug_check
)
for f in files:
    source_file = f["source_file"]
    period      = f["period"]
    Sequence    = f["sequence"]

print(f"run_monthly - {run_monthly}  run_mid_monthly - {run_mid_monthly}  period - {period}  Sequence - {Sequence}  source_file - {source_file}")

# COMMAND ----------

from datetime import datetime
invoice_run_dt = datetime.now().strftime("%Y%m%d")
print(invoice_run_dt)

# COMMAND ----------

# DBTITLE 1,SF Connect
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

# MAGIC %md
# MAGIC ## ### MID_MONTHLY

# COMMAND ----------

if run_mid_monthly == 'true':
  from pyspark.sql.functions import lit
  period_short = period[:3] + period[5:7] + period[-1]
  print(period_short)
  df_INV = spark.read \
    .format("snowflake") \
    .options(**options) \
    .option("query",  f"""select distinct invoice_no,account_no from {sfDatabase}.PRS_CEG_EXT.CALLIDUS_INVOICE_PDF_MIDMONTHLY where trim(PERIOD_NAME) = '{period_short}'""").load()
  
  if not df_INV.head(1):
    print(f"No record present for this {period}")
    dbutils.notebook.exit("No record present for this period")
  
  df_inv_status = df_INV.withColumn("STATUS", lit("N"))
  display(df_inv_status)

# COMMAND ----------

if run_mid_monthly == 'true':
    from pyspark.sql.functions import when
    from datetime import datetime, timedelta
    import re
    # Get current and previous period strings
    # period_file = re.sub(r'\s*M$', '', period) 
    # period_file = re.sub(r'(\w+)\s+(\d{4})', lambda m: m.group(1)[:3].upper() + m.group(2), period_file)
    # print(period_file,period)

    # Collect invoice numbers from DataFrame
    inv_acc_list = [(row["INVOICE_NO"], row["ACCOUNT_NO"]) for row in df_inv_status.collect()]
    print(inv_acc_list)
    status_updates = []

    remove_files_by_extension(f"{mft_out}/aldm/outbound_1/CALLIDUS/INVOICE", ".pdf")

    INVOICE_TYPE = "MID_MONTHLY"

    # Run notebook for each invoice and track status
    for invoice, account_no in inv_acc_list:
        try:
            result = dbutils.notebook.run(
                "/Workspace/Repos/Aldm Repository/ALDM/databricks/transformer/aldm_callidus/nb-Invoice_Generic",
                timeout_seconds=600,
                arguments={
                "invoice_no": str(invoice),
                "extract_filename_format": f"{INVOICE_TYPE}_INVOICE_PDF_{account_no}_{period}_{Sequence}_APPROVED_{invoice_run_dt}.pdf",
                "fs_dir_path": "callidus/INVOICE/",
                "interface_home": "CALLIDUS/INVOICE",
                "invoice_type": INVOICE_TYPE,
                "period" : period_short,
            }
            )
            if result is None:
                status_updates.append((invoice, 'Y'))  # Success
            elif result.strip().lower() != "success":
                raise RuntimeError(f"Notebook run failed for invoice {invoice}: {result}")
            else:
                status_updates.append((invoice, 'Y'))  # Success
            print(f"{INVOICE_TYPE}_INVOICE_PDF_{account_no}_{period}_{Sequence}_APPROVED_{invoice_run_dt}.pdf")
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
        merge_and_archive_pdfs_mft("CALLIDUS/INVOICE", "CALLIDUS/INVOICE/FINAL", f"{INVOICE_TYPE}_INVOICE_PDF_{period}_{Sequence}_APPROVED_{invoice_run_dt}.pdf",INVOICE_TYPE)
        archive_files_at_azure_share(fs_conn_str, fs_name, "callidus/INVOICE", INVOICE_TYPE)
        move_files_to_azure_share(fs_conn_str, fs_name, f"{mft_out}/aldm/outbound_1/CALLIDUS/INVOICE/INDIVIDUAL", "callidus/INVOICE", INVOICE_TYPE)
        move_files_to_azure_share(fs_conn_str, fs_name, f"{mft_out}/aldm/outbound_1/CALLIDUS/INVOICE/FINAL", "callidus/INVOICE", INVOICE_TYPE)
    except Exception as e:
        print(f"Error in merging or moving files: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### MONTHLY 

# COMMAND ----------

if run_monthly == 'true':
  from pyspark.sql.functions import lit
  df_INV= spark.read \
    .format("snowflake") \
    .options(**options) \
    .option("query",  f"""select distinct invoice_no,account_no from {sfDatabase}.PRS_CEG_EXT.CALLIDUS_INVOICE_PDF_MONTHLY where trim(PERIOD_NAME) = '{period}' limit 4""").load()

  if not df_INV.head(1):
    print(f"No record present for this {period}")
    dbutils.notebook.exit("No record present for this period")

  df_inv_status_monthly = df_INV.withColumn("STATUS", lit("N"))
  display(df_inv_status_monthly)

# COMMAND ----------

if run_monthly == 'true':
    from pyspark.sql.functions import when
    from datetime import datetime, timedelta
    import re
    # Get current and previous period strings
    period_file = re.sub(r'\s*M$', '', period) 
    period_file = re.sub(r'(\w+)\s+(\d{4})', lambda m: m.group(1)[:3].upper() + m.group(2), period_file)
    print(period_file,period)

    # Collect invoice numbers from DataFrame
    inv_acc_list = [(row["INVOICE_NO"], row["ACCOUNT_NO"]) for row in df_inv_status_monthly.collect()]
    print(inv_acc_list)
    status_updates = []

    remove_files_by_extension(f"{mft_out}/aldm/outbound_1/CALLIDUS/INVOICE", ".pdf")

    INVOICE_TYPE = "MONTHLY"


    # Run notebook for each invoice and track status
    for invoice, account_no in inv_acc_list:
        try:
            result = dbutils.notebook.run(
                "/Workspace/Repos/Aldm Repository/ALDM/databricks/transformer/aldm_callidus/nb-Invoice_Generic",
                timeout_seconds=600,
                arguments={
                "invoice_no": str(invoice),
                "extract_filename_format": f"{INVOICE_TYPE}_INVOICE_PDF_{account_no}_{period_file}_{Sequence}_APPROVED_{invoice_run_dt}.pdf",
                "fs_dir_path": "callidus/INVOICE/",
                "interface_home": "CALLIDUS/INVOICE",
                "invoice_type": INVOICE_TYPE,
                "period": period,
                }
            )
            if result is None:
                status_updates.append((invoice, 'Y'))  # Success
            elif result.strip().lower() != "success":
                raise RuntimeError(f"Notebook run failed for invoice {invoice}: {result}")
            else:
                status_updates.append((invoice, 'Y'))  # Success
            print(f"{INVOICE_TYPE}_INVOICE_PDF_{account_no}_{period_file}_{Sequence}_APPROVED_{invoice_run_dt}.pdf")
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
        merge_and_archive_pdfs_mft("CALLIDUS/INVOICE", "CALLIDUS/INVOICE/FINAL", f"{INVOICE_TYPE}_INVOICE_PDF_{period_file}_{Sequence}_APPROVED_{invoice_run_dt}.pdf", INVOICE_TYPE)
        archive_files_at_azure_share(fs_conn_str, fs_name, "callidus/INVOICE", INVOICE_TYPE)
        move_files_to_azure_share(fs_conn_str, fs_name, f"{mft_out}/aldm/outbound_1/CALLIDUS/INVOICE/INDIVIDUAL", "callidus/INVOICE", INVOICE_TYPE)
        move_files_to_azure_share(fs_conn_str, fs_name, f"{mft_out}/aldm/outbound_1/CALLIDUS/INVOICE/FINAL", "callidus/INVOICE", INVOICE_TYPE)
    except Exception as e:
        print(f"Error in merging or moving files: {e}")
        raise