# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver layer Processing

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable


# COMMAND ----------

df_silver = spark.read \
    .format("delta") \
    .load("s3://pfizerbucket01/processed_data/bronze/mfg_deviation/")

df_silver.count()

# COMMAND ----------

display(df_silver)


# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.dropna(subset=["deviation_id","batch_id","equipment_id","deviation_type","severity","description","root_cause","corrective_action","reported_by","reported_date","closure_date","status"])

df_silver.count()

# COMMAND ----------

df_silver = df_silver.withColumn("reported_by",
            F.when(F.col("reported_by").isNull(), F.lit("UNKNOWN"))
             .otherwise(F.trim(F.col("reported_by")))
        ) \
        .withColumn("description",
            F.when(F.col("description").isNull(), F.lit("NO_DESCRIPTION"))
             .otherwise(F.trim(F.col("description")))
        ) \
        .withColumn("equipment_id",
            F.when(F.col("equipment_id").isNull(), F.lit("NOT_APPLICABLE"))
             .otherwise(F.trim(F.col("equipment_id")))
        ) \
        .withColumn("root_cause",
            F.when(F.col("root_cause").isNull(), F.lit("UNDER_INVESTIGATION"))
             .otherwise(F.trim(F.col("root_cause")))
        ) \
        .withColumn("corrective_action",
            F.when(F.col("corrective_action").isNull(), F.lit("PENDING"))
             .otherwise(F.trim(F.col("corrective_action")))
        )

# COMMAND ----------

df_silver = df_silver.withColumn("reported_month",
            F.date_format("reported_date", "yyyy-MM")
        ) \
        .withColumn("reported_year",
            F.year("reported_date").cast("string")
        ) \
        .withColumn("reported_quarter",
            F.concat(
                F.year("reported_date").cast("string"),
                F.lit("-Q"),
                F.quarter("reported_date").cast("string")
            )
        )

# COMMAND ----------

SLA_CRITICAL = 3
SLA_MAJOR    = 15
SLA_MINOR    = 30

df_silver = df_silver.withColumn("sla_days",
        F.when(F.col("severity") == "CRITICAL", F.lit(SLA_CRITICAL))
         .when(F.col("severity") == "MAJOR",    F.lit(SLA_MAJOR))
         .when(F.col("severity") == "MINOR",    F.lit(SLA_MINOR))
         .otherwise(F.lit(SLA_MINOR))
)



# COMMAND ----------

df_silver = df_silver.withColumn("days_to_closure",
        F.when(
            F.col("closure_date").isNotNull(),
            F.datediff(F.col("closure_date"), F.col("reported_date"))
        ).otherwise(F.lit(None))
    )

display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("silver_processed_at", F.current_timestamp())


df_silver = df_silver.select("deviation_id","batch_id","equipment_id","deviation_type","severity","description","root_cause","corrective_action","reported_by","reported_date","closure_date","status","sla_days","days_to_closure","silver_processed_at")

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

target_path = "s3://pfizerbucket01/processed_data/silver/mfg_deviation/"

if DeltaTable.isDeltaTable(spark, target_path):
    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("t")
        .merge(
            df_silver.alias("s"),
            "t.deviation_id = s.deviation_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
        df_silver.write \
                .format("delta") \
                .option("delta.enableChangeDataFeed", "true") \
                .mode("overwrite") \
                .save(target_path)