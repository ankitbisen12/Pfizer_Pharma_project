# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver layer Processing

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable


# COMMAND ----------

df_silver = spark.read \
    .format("delta") \
    .load("s3://pfizerbucket01/processed_data/bronze/mfg_order/")

df_silver.count()

# COMMAND ----------

display(df_silver)


# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

# Mark column as null which has either batch_id ,order_id,durg_code as null  

df_silver = df_silver.withColumn("null_flag",
                F.when(
                    F.col('batch_id').isNull() |
                    F.col('order_id').isNull() |
                    F.col('drug_code').isNull()
                    ,True
                ).otherwise(False)
)          



# COMMAND ----------

# Converting columns to upper case
df_silver = df_silver.withColumn("status",F.upper(F.col("status")))\
                     .withColumn("priority", F.upper(F.col("priority")))

# COMMAND ----------

df_silver.select(F.col("status")).distinct().show()
df_silver.select(F.col("priority")).distinct().show()



# COMMAND ----------

display(df_silver.limit(10))

# COMMAND ----------

# 
VALID_STATUS = ["PLANNED", "IN PROGRESS", "DONE","CLOSED","CANCELLED"]
VALID_PRIORITY = ["HIGH", "MEDIUM", "LOW"]

df_silver = df_silver.withColumn("status",F.when(F.col("status").isin(VALID_STATUS),F.col('status')).otherwise("UNKNOWN"))\
                     .withColumn("priority",F.when(F.col("priority").isin(VALID_PRIORITY),F.col('priority')).otherwise("UNKNOWN")    
)
                     
display(df_silver.filter(F.col("priority") == "UNKNOWN"))

# COMMAND ----------

df_silver = df_silver.dropna(subset=["start_date","end_date","actual_qty","planned_qty"])

df_silver.count()

# COMMAND ----------

df_silver = df_silver.withColumn(
                "date_invalid_flag",
                F.when(F.col("start_date") > F.col("end_date"), True).otherwise(False)
            ).withColumn(
                "date_month",
                F.date_format(F.col("start_date"), "dd MMMM")
            ).withColumn(
                "Year",
                F.date_format(F.col("start_date"), "yyyy")
            ).withColumn("order_quarter",
                F.concat(
                    F.year("start_date").cast("string"),
                    F.lit("-Q"),
                    F.quarter("start_date").cast("string")
                )
)

display(df_silver)

# COMMAND ----------

df_silver = (
    df_silver.filter((F.col("actual_qty") != 0) &(F.col("planned_qty") != 0))
             .withColumn("qty_variance",F.round(F.col("actual_qty") - F.col("planned_qty"),2))
             .withColumn("qty_variance_pct",F.round((F.col("qty_variance") / F.col("planned_qty")) * 100, 2))
             .withColumn("fulfillment_status",
                    F.when(F.col("actual_qty") > F.col("planned_qty"), "OK")
                     .when(F.col("actual_qty") < F.col("planned_qty"), "UNDER PRODUCTION")
                     .otherwise("ON TARGET")
             )
             .withColumn(
                    "planned_duration_days",
                        F.datediff(F.col("end_date"), F.col("start_date"))
             )
)

display(df_silver)

# COMMAND ----------

df_silver = df_silver.filter(
    (F.col("date_invalid_flag") == False) &
    (F.col("null_flag") == False)
)

df_silver.count()


# COMMAND ----------

df_silver = df_silver.select("order_id", "batch_id","drug_code","planned_qty","actual_qty","start_date","end_date","status","priority","created_by","created_at","date_month","Year","order_quarter","qty_variance","qty_variance_pct","fulfillment_status","planned_duration_days")

display(df_silver)



# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

target_path = "s3://pfizerbucket01/processed_data/silver/mfg_order/"

if DeltaTable.isDeltaTable(spark, target_path):
    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("t")
        .merge(
            df_silver.alias("s"),
            "t.order_id = s.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_silver.write \
        .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .mode("append") \
        .save(target_path)