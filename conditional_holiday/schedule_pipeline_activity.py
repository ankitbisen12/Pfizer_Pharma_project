from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Using AWS bucket & databricks backed secret scopes

# schedule_holidays = 'schedule_holidays'
url = dbutils.secrets.get("bucket", "holiday-csv-path")
base_path = f"{url}"
# print(base_path)

# dbutils.secrets.list("bucket")

# Reading data from AWS S3 bucket 
df =(spark.read.format('csv')
      .option("header",True)
      .option("inferSchema",True)
      .load(base_path)
)


# Date formatting
df = df.withColumn("Date", F.date_format(F.col("Date"),"yyyy-MM-dd")) 
display(df)


# Read Value from previous task
current_date = dbutils.jobs.taskValues.get(
    taskKey = "get_run_day",
    key="input_date",
    debugValue = "2026-01-01"     # fallback for testing
)

is_present = df.filter(F.col("Date") ==  current_date).count() >0
print(f"Date Present: {is_present}")


#--------------------Control Flow-------------------#

if is_present:
     dbutils.notebook.exit("RUN_MAIN")
else:
    dbutils.notebook.exit("SKIP")