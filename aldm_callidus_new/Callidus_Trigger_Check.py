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

options = dict(sfUrl = f"{sfUrl}",sfUser = f"{sfUser}",
               pem_private_key = f"{pem_private_key}",sfDatabase = f"{sfDatabase}",
               sfSchema = f"{sf_target_schema}",sfWarehouse = f"{sfWarehouse}")

# COMMAND ----------

# import time

# time.sleep(4 * 60)

# COMMAND ----------

# df_SF= spark.read \
#   .format("snowflake") \
#   .options(**options) \
#   .option("query",  """SELECT * FROM DEV_IDW.ATO_ADX_STG.CALLIDUS_TRIGGER_TEMP WHERE RUN_ID = (select max(RUN_ID) from DEV_IDW.ATO_ADX_STG.CALLIDUS_TRIGGER_TEMP)""").load()


# display(df_SF)

# COMMAND ----------

df_SF = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("query", """SELECT * FROM SIT_IDW.ATO_ADX_STG.CALLIDUS_TRIGGER_TEMP123""").load()

# display(df_SF)

# COMMAND ----------

# DBTITLE 1,Cell 6
from pyspark.sql.functions import lit, current_timestamp

df_tigger = spark.read.table("ALDM_STAGING.CALLIDUS_TIGGER")

df_not_in_tigger = df_SF.join(df_tigger, on="RUN_ID", how="left_anti").select(*df_SF.columns)
df_not_in_tigger.show()
df_to_append = df_not_in_tigger.withColumn("RUN_FLAG", lit("N")) \
    .withColumn("STATUS", lit("Pending")) \
    .withColumn("INSERT_TS", current_timestamp()) \
    .withColumn("UPDATE_TS", lit(None).cast("timestamp"))

df_to_append.select(
    df_to_append["RUN_ID"].cast("string").alias("RUN_ID"),
    "PERIOD_NAME", "PERIOD_TYPE", "RUN_TIMESTAMP", "RUN_FLAG", "STATUS", "INSERT_TS", "UPDATE_TS"
).write.mode("append").saveAsTable("ALDM_STAGING.CALLIDUS_TIGGER")

# display(df_to_append)

# COMMAND ----------

from pyspark.sql.functions import col

df_latest_N = spark.read.table("ALDM_STAGING.CALLIDUS_TIGGER") \
    .filter(col("RUN_FLAG") == "N") \
    .orderBy(col("RUN_TIMESTAMP").asc()) \
    .limit(1)

display(df_latest_N)

# COMMAND ----------

df_latest_N.writeTo("aldm_staging.calludis_current_trigger_run").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aldm_staging.calludis_current_trigger_run;

# COMMAND ----------

from pyspark.sql.functions import upper
period_type_value = (
    df_latest_N.select(upper("PERIOD_TYPE").alias("PERIOD_TYPE")).first()["PERIOD_TYPE"]
    if df_latest_N.count() > 0 else "NULL"
)
print(period_type_value)
dbutils.jobs.taskValues.set(key="PERIOD_TYPE", value=period_type_value)

# COMMAND ----------

from pyspark.sql.functions import trim
period_name = (
    df_latest_N.select(trim("PERIOD_NAME").alias("PERIOD_NAME")).first()["PERIOD_NAME"]
    if df_latest_N.count() > 0 else "NULL"
)

print(period_name)
dbutils.jobs.taskValues.set(key="PERIOD_NAME", value=period_name)

# COMMAND ----------

# period_name_value = dbutils.jobs.taskValues.get(taskKey="Callidus_Trigger_Check", key="PERIOD_NAME", debugValue="DEBUG")
# print(period_name_value)

# COMMAND ----------

# spark.sql("""
# UPDATE ALDM_STAGING.CALLIDUS_TIGGER
# SET RUN_FLAG = 'Y', update_ts = current_timestamp(), STATUS = 'Skipped'
# WHERE PERIOD_TYPE IN (SELECT PERIOD_TYPE FROM aldm_staging.calludis_current_trigger_run)
#   AND RUN_FLAG = 'N'
# """)

# COMMAND ----------

# spark.sql("""
# UPDATE ALDM_STAGING.CALLIDUS_TIGGER
# SET STATUS = 'Completed'
# WHERE run_ID IN (SELECT RUN_ID FROM aldm_staging.calludis_current_trigger_run)
# """)

# COMMAND ----------

# %sql
# CREATE TABLE ALDM_STAGING.CALLIDUS_TIGGER (
#   RUN_ID STRING,
#   PERIOD_NAME STRING,
#   PERIOD_TYPE STRING,
#   RUN_TIMESTAMP TIMESTAMP,
#   RUN_FLAG STRING,
#   STATUS STRING,
#   INSERT_TS TIMESTAMP,
#   UPDATE_TS TIMESTAMP
# )

# COMMAND ----------

# %sql
# DELETE FROM ALDM_STAGING.CALLIDUS_TIGGER;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ALDM_STAGING.CALLIDUS_TIGGER;

# COMMAND ----------

# %sql
# update ALDM_STAGING.CALLIDUS_TIGGER 
# set PERIOD_NAME = 'JUN26D'
# where RUN_ID = 2;

# COMMAND ----------

# %sql
# update ALDM_STAGING.CALLIDUS_TIGGER 
# set RUN_FLAG = 'N' ,
#  status = null,
#  insert_ts = null,
#  update_ts = null
#   where RUN_ID in (2)

# COMMAND ----------

# %sql
# update ALDM_STAGING.CALLIDUS_TIGGER 
# set RUN_FLAG = 'N' ,
#  status = null,
#  insert_ts = null,
#  update_ts = null
#   where RUN_ID in (3)

# COMMAND ----------

# from pyspark.sql.functions import current_date

# if df_SF.filter(df_SF['run_date'] == current_date()).count() > 0:
#     dbutils.jobs.taskValues.set(key="Calledius_trigger", value="SUCCESS")
# else:
#     dbutils.jobs.taskValues.set(key="Calledius_trigger", value="FAILED")

# COMMAND ----------

# DBTITLE 1,Cell 6
# from pyspark.sql.functions import current_date

# if df_SF.filter(df_SF['run_date'] == current_date()).count() > 0:
#     print("Success" )
#     dbutils.jobs.taskValues.set(key="Calledius_trigger", value="SUCCESS")
# else:
#     print("Failed")
#     dbutils.jobs.taskValues.set(key="Calledius_trigger", value="FAILED")

# # value = dbutils.jobs.taskValues.get(taskKey=dbutils.taskContext().taskKey(), key="Calledius_trigger", debugValue="DEBUG")
# # display(value)  # This will show whether the value is "SUCCESS" or "FAILED"