# Databricks notebook source
# MAGIC %md
# MAGIC Files Come's to ADLS when we receive a confirmation email via Logic app
# MAGIC Name - Callidus_email_trigger

# COMMAND ----------

import re

from bs4 import BeautifulSoup
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
# from pyspark.sql.functions import col, current_timestamp, lower, trim, udf


SOURCE_PATH = "/mnt/aldm-home/callidus_email_files/input/"
TARGET_TABLE = "aldm_staging.callidus_email_ingest"

# COMMAND ----------

files = [f for f in dbutils.fs.ls(SOURCE_PATH) if f.name.endswith(".json")]

if not files:
    dbutils.jobs.taskValues.set(key="has_pending", value="false")
    dbutils.jobs.taskValues.set(key="pending_rows", value=[])
    dbutils.notebook.exit("No new files")

# COMMAND ----------

# Remove the warnings and other not related text from email
NOISE = (
    "caution",
    "external email",
    "email-disclaimer",
    "proprietary and confidential",
    "amdocs development centre",
    "amdocs policy statement",
)
# SIGNATURE_START = re.compile(r"^(thanks and regards|best regards|regards|thanks)\b", re.I)

# Everything from the first of these lines onwards is signature or legal footer and is not of any buisness use.
CONTENT_END = re.compile(
    r"^("
    r"thanks and regards|best regards|kind regards|regards|thanks"
    r"|this message|this e-?mail|this transmission"
    r"|disclaimer"
    r"|hutchison 3g|vodafonethree"
    r"|amdocs development centre"
    r"|registered in england"
    r")\b",
    re.I,
)


def main_content(html):
    if not html:
        return ""
    text = BeautifulSoup(html, "html.parser").get_text("\n")
    lines = []
    for line in (ln.strip() for ln in text.splitlines()):
        if not line or any(n in line.lower() for n in NOISE):
            continue
        # if SIGNATURE_START.match(line):
        if CONTENT_END.match(line):
            break
        lines.append(line)
    return "\n".join(lines)

# COMMAND ----------

# clean_body = udf(main_content, StringType())

# # Subject format: Oracle GSI Commission Payment File Generation <date> <period> <sequence>
# SUBJECT_PATTERN = (
#     r"(?i)Oracle GSI Commission Payment File Generation\s+"
#     r"(\d{4}-\d{2}-\d{2})\s+"
#     r"(.+?)\s+"
#     r"(\d+)\s*$"
# )

# staged = (
#     spark.read.option("multiLine", True)
#     .json(SOURCE_PATH)
#     .selectExpr("*", "_metadata.file_name AS source_file")
#     .withColumn("mail_body", clean_body("mail_body"))
#     .withColumn("insert_ts", current_timestamp())
#     .withColumn("file_date", to_date(regexp_extract(col("mail_subject"), SUBJECT_PATTERN, 1)))
#     .withColumn("period", regexp_extract(col("mail_subject"), SUBJECT_PATTERN, 2))
#     .withColumn("File_Sequence", regexp_extract(col("mail_subject"), SUBJECT_PATTERN, 3))
#     .select(
#         "mail_from",
#         "mail_subject",
#         "period",
#         "File_Sequence",
#         "mail_body",
#         col("mail_received").alias("mail_received_time"),
#         "source_file",
#         "insert_ts",
#     )
#     .filter(~lower(trim(col("mail_subject"))).rlike(r"^(re|fw|fwd)\s*:"))
# )

# COMMAND ----------

clean_body = udf(main_content, StringType())

# Subject format: Oracle GSI Commission Payment File Generation <date> <period> <sequence>
SUBJECT_PREFIX = r"(?i)Oracle GSI Commission Payment File Generation\s+"
DATE_PATTERN = SUBJECT_PREFIX + r"(\d{1,4}[-/]\d{1,2}[-/]\d{1,4})"
TAIL_PATTERN = SUBJECT_PREFIX + r"[\d/-]+\s+(.+?)\s+(\d+)\s*$"

staged = (
    spark.read.option("multiLine", True)
    .json(SOURCE_PATH)
    .selectExpr("*", "_metadata.file_name AS source_file")
    .withColumn("mail_body", clean_body("mail_body"))
    .withColumn("insert_ts", current_timestamp())
    .withColumn("raw_date", regexp_extract(col("mail_subject"), DATE_PATTERN, 1))
    .withColumn("file_date",
        coalesce(
            expr("try_to_date(raw_date, 'yyyy-MM-dd')"),
            expr("try_to_date(raw_date, 'dd-MM-yyyy')"),
            expr("try_to_date(raw_date, 'yyyy/MM/dd')"),
            expr("try_to_date(raw_date, 'dd/MM/yyyy')"),
        ),)
    .withColumn("period", regexp_extract(col("mail_subject"), TAIL_PATTERN, 1))
    .withColumn("File_Sequence", regexp_extract(col("mail_subject"), TAIL_PATTERN, 2))
    .select(
        "mail_from",
        "mail_subject",
        "file_date",
        "period",
        "File_Sequence",
        "mail_body",
        col("mail_received").alias("mail_received_time"),
        "source_file",
        "insert_ts",
    )
    .filter(~lower(trim(col("mail_subject"))).rlike(r"^(re|fw|fwd)\s*:"))
)

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS aldm_staging.callidus_email_ingest (
  mail_from STRING,
  mail_subject STRING,
  period STRING,
  File_Sequence STRING, 
  mail_body STRING,
  mail_received_time TIMESTAMP,
  source_file STRING,
  insert_ts TIMESTAMP,
  Processed STRING
)
""")

## Merge into the target table for auditing
staged.createOrReplaceTempView("staged_emails")
spark.sql(
    f"""
    MERGE INTO {TARGET_TABLE} AS t
    USING staged_emails AS s
        ON t.source_file = s.source_file
        AND t.mail_received_time = s.mail_received_time
    WHEN NOT MATCHED THEN INSERT (
    mail_from, mail_subject, period, File_Sequence, mail_body,
    mail_received_time, source_file, insert_ts, Processed
) VALUES (
    s.mail_from, s.mail_subject, s.period, s.File_Sequence, s.mail_body,
    s.mail_received_time, s.source_file, s.insert_ts, 'N'
)
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC Set Varible in Job pipline for further updates

# COMMAND ----------

# pending = spark.sql(
#     f"""
#     SELECT source_file, mail_received_time, mail_subject, period, File_Sequence
#     FROM {TARGET_TABLE}
#     WHERE Processed = 'N'
#     ORDER BY mail_received_time
#     """
# ).collect()

# pending_rows = [
#     {
#         "source_file": r.source_file,
#         "mail_received_time": str(r.mail_received_time),
#         "mail_subject": r.mail_subject,
#         "period": r.period,
#         "file_sequence": r.File_Sequence,
#     }
#     for r in pending
# ]

# dbutils.jobs.taskValues.set(key="has_pending", value="true" if pending_rows else "false")
# dbutils.jobs.taskValues.set(key="pending_rows", value=pending_rows)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aldm_staging.callidus_email_ingest

# COMMAND ----------

# A trailing standalone "M" marks the monthly run. No month name ends in M,
RUN_PERIOD_EXPR = """
    CASE
      WHEN period IS NULL OR trim(period) = '' THEN NULL
      WHEN trim(period) RLIKE '(?i)M$'         THEN 'MONTHLY'
      ELSE 'MID MONTHLY'
    END
"""

pending = spark.sql(
    f"""
    SELECT source_file, mail_received_time, mail_subject, period, File_Sequence,
           {RUN_PERIOD_EXPR} AS run_period
    FROM {TARGET_TABLE}
    WHERE Processed = 'N'
    ORDER BY mail_received_time desc Limit 1
    """
).collect()

# An unparsed subject is skipped here and stays pending for a later run.
classified = [r for r in pending if r.run_period is not None]

kinds = {r.run_period for r in classified}
# in_execution = [r.source_file for r in classified]
in_execution = [
    {"source_file": r.source_file, "period": r.period, "sequence": r.File_Sequence}
    for r in classified
]

dbutils.jobs.taskValues.set(key="run_monthly", value="true" if "MONTHLY" in kinds else "false")
dbutils.jobs.taskValues.set(key="run_mid_monthly", value="true" if "MID MONTHLY" in kinds else "false")
dbutils.jobs.taskValues.set(key="in_execution", value=in_execution)

print(f"pending={len(pending)}  in_execution={len(in_execution)}  kinds={sorted(kinds)}")