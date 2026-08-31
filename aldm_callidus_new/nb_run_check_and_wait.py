# Databricks notebook source
from time import sleep
from datetime import datetime, timedelta, time 
from zoneinfo import ZoneInfo

UK = ZoneInfo("Europe/London")
CUTOFF = time(13, 30)
RETRY_AT = time(0, 1)   


def is_working_day(d):
    return spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM aldm_staging.calendar_holiday
        WHERE TO_DATE(DATE) = DATE '{d}'
          AND holidayflag = 'N'
    """).collect()[0][0] > 0


def next_working_day(after):
    return spark.sql(f"""
        SELECT MIN(TO_DATE(DATE)) AS d
        FROM aldm_staging.calendar_holiday
        WHERE holidayflag = 'N'
          AND TO_DATE(DATE) > DATE '{after}'
    """).collect()[0][0]

# COMMAND ----------

while True:
    now = datetime.now(UK)

    if is_working_day(now.date()) and now.time() < CUTOFF:
        print(f"In execution window at {now} - proceeding")
        break

    nxt = next_working_day(now.date())
    if nxt is None:
        raise Exception(f"No working day after {now.date()} in calendar_holiday")

    target = datetime.combine(nxt, RETRY_AT, tzinfo=UK)
    wait_s = (target - datetime.now(UK)).total_seconds()
    print(f"Not runnable now. Sleeping {wait_s/3600:.1f}h until {target}")
    sleep(max(wait_s, 0))