# Databricks notebook source
# MAGIC %md
# MAGIC ### # Marking RUN_FLAG = Y and STATUS = Completed for Current Execution Trigger

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

spark.sql("""
UPDATE ALDM_STAGING.CALLIDUS_TIGGER
SET STATUS = 'Completed', RUN_FLAG = 'Y', update_ts = current_timestamp()
WHERE run_ID IN (SELECT RUN_ID FROM aldm_staging.calludis_current_trigger_run)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC update ALDM_STAGING.CALLIDUS_TIGGER 
# MAGIC set RUN_FLAG = 'N',
# MAGIC  status = null,
# MAGIC  update_ts = null
# MAGIC   where RUN_ID in (5,6,7,8,9,10,11) 
# MAGIC   and 1 = (select case when run_id in (11) then 1 else 0 end from aldm_staging.calludis_current_trigger_run)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT RUN_ID FROM aldm_staging.calludis_current_trigger_run;

# COMMAND ----------

# %sql
# update ALDM_STAGING.CALLIDUS_TIGGER 
# set RUN_FLAG = 'N',
#  status = null,
#  update_ts = null
#   where RUN_ID  in (9) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ALDM_STAGING.CALLIDUS_TIGGER;