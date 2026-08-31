# Databricks notebook source
## Set variables
TARGET_TABLE = "aldm_staging.callidus_email_ingest"
SOURCE_PATH = "/mnt/aldm-home/callidus_email_files/input/"
ARCHIVE_PATH = "/mnt/aldm-home/callidus_email_files/archive/"

# COMMAND ----------

# MAGIC %md
# MAGIC Update the processed row to 'Y' after the pdf/extract is generated in target table 'aldm_staging.callidus_email_ingest'

# COMMAND ----------

files = dbutils.jobs.taskValues.get(
    taskKey="EMAIL_INGEST", key="in_execution", default=[], debugValue=[]
)

# Task value may be a list of names or a list of dicts.
names = sorted({f["source_file"] if isinstance(f, dict) else f for f in files})

if names:
    spark.createDataFrame([(n,) for n in names], "source_file STRING") \
         .createOrReplaceTempView("processed_rows")
    spark.sql(
        f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING processed_rows AS s
          ON t.source_file = s.source_file
        WHEN MATCHED AND t.Processed = 'N' THEN UPDATE SET t.Processed = 'Y'
        """
    )

print(f"marked {len(names)} file(s) processed")

# COMMAND ----------

# MAGIC %md
# MAGIC Move the processed files to archive  '/mnt/aldm-home/callidus_email_files/archive/

# COMMAND ----------

ingested = {f["source_file"] for f in files}

for f in dbutils.fs.ls(SOURCE_PATH):
    if f.name in ingested:
        dbutils.fs.mv(f.path, ARCHIVE_PATH + f.name)