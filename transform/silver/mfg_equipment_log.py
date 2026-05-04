# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver layer Processing

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable


# COMMAND ----------

df_silver = spark.read \
    .format("delta") \
    .load("s3://pfizerbucket01/processed_data/bronze/mfg_equipment_lot/")

df_silver.count()

# COMMAND ----------

display(df_silver)


# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.dropna(subset=["equipment_id","plant_id","event_type","event_start","event_end","downtime_minutes","calibration_status","operator_id","remarks"])

df_silver.count()

# COMMAND ----------

df_silver = df_silver.withColumn("operator_id",
            F.when(F.col("operator_id").isNull(), F.lit("SYSTEM"))
             .otherwise(F.col("operator_id"))
        ) \
        .withColumn("calibration_status",
            F.when(F.col("calibration_status").isNull(), F.lit("UNKNOWN"))
             .otherwise(F.col("calibration_status"))
        ) \
        .withColumn("remarks",
            F.when(F.col("remarks").isNull(), F.lit("NO_REMARKS"))
             .otherwise(F.trim(F.col("remarks")))
        )

# COMMAND ----------

df_silver = df_silver.withColumn(
                "start_date_month",
                F.date_format(F.col("event_start"), "dd MMMM")
            ).withColumn(
                "start_Year",
                F.date_format(F.col("event_start"), "yyyy")
            ).withColumn("order_quarter",
                F.concat(
                    F.year("event_start").cast("string"),
                    F.lit("-Q"),
                    F.quarter("event_start").cast("string")
                )
)

df_silver = df_silver.withColumn(
                "end_date_month",
                F.date_format(F.col("event_end"), "dd MMMM")
            ).withColumn(
                "end_Year",
                F.date_format(F.col("event_end"), "yyyy")
            ).withColumn("order_quarter",
                F.concat(
                    F.year("event_end").cast("string"),
                    F.lit("-Q"),
                    F.quarter("event_end").cast("string")
                )
)
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("event_type",
            F.upper(F.trim(F.col("event_type")))
) 

# COMMAND ----------

df_silver = df_silver.withColumn("event_duration_minutes",
        F.when(
            F.col("event_end").isNotNull(),
            (F.unix_timestamp("event_end") - F.unix_timestamp("event_start")) / 60
        ).otherwise(F.lit(None))  # null = still running
    )

df_silver = df_silver.withColumn("event_duration_hours",
            F.round(F.col("event_duration_minutes") / 60, 2)
        ) \
        .withColumn("event_duration_category",
            F.when(F.col("event_duration_minutes").isNull(),"ONGOING")
             .when(F.col("event_duration_minutes") < 60,"SHORT")
             .when(F.col("event_duration_minutes") <= 480,"MEDIUM")
             .otherwise("LONG")
        )
 
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("silver_processed_at", F.current_timestamp())


df_silver = df_silver.select("equipment_id","plant_id","event_type","event_start","downtime_minutes","calibration_status","operator_id","remarks","silver_processed_at","start_date_month","start_Year","order_quarter","end_date_month","end_Year","event_duration_minutes","event_duration_hours","event_duration_category")

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

target_path = "s3://pfizerbucket01/processed_data/silver/mfg_equipment_log/"

df_silver.write \
        .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .mode("overwrite") \
        .save(target_path)