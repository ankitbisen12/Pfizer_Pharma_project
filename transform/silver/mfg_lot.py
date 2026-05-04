# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver layer Processing

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable


# COMMAND ----------

df_silver = spark.read \
    .format("delta") \
    .load("s3://pfizerbucket01/processed_data/bronze/mfg_lot/")

df_silver.count()

# COMMAND ----------

display(df_silver)


# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.dropna(subset=["batch_id","lot_id","drug_code","plant_id"])

df_silver.count()

# COMMAND ----------

df_silver = df_silver.withColumn("operator_id",
            F.when(F.col("operator_id").isNull(), F.lit("UNKNOWN"))
             .otherwise(F.trim(F.col("operator_id")))
        ) \
        .withColumn("equipment_id",
            F.when(F.col("equipment_id").isNull(), F.lit("NOT_ASSIGNED"))
             .otherwise(F.trim(F.col("equipment_id")))
        ) \
        .withColumn("process_step",
            F.when(F.col("process_step").isNull(), F.lit("UNKNOWN"))
             .otherwise(F.trim(F.col("process_step")))
        ) \
        .withColumn("deviation_flag",
            F.when(F.col("deviation_flag").isNull(), F.lit(False))
             .otherwise(F.col("deviation_flag"))
)

# COMMAND ----------

df_silver = df_silver.withColumnRenamed("manufacture_date","start_timestamp"
        )\
        .withColumn("manufacture_month",
            F.date_format("start_timestamp", "yyyy-MM")
        ) \
        .withColumn("manufacture_year",
            F.year("start_timestamp").cast("string")
        ) \
        .withColumn("manufacture_quarter",
            F.concat(
                F.year("start_timestamp").cast("string"),
                F.lit("-Q"),
                F.quarter("start_timestamp").cast("string")
            )
        ) \
        .withColumn("shift_code",
            F.when(F.hour("start_timestamp").between(6,  13), "Morning")
             .when(F.hour("start_timestamp").between(14, 21), "Afternoon")
             .otherwise("Night")
        ) \
        .withColumn("day_of_week",
            F.date_format("start_timestamp", "EEEE")
        )

# COMMAND ----------

df_silver = df_silver.withColumn("material_loss_qty",
            F.when(
                F.col("input_material_qty").isNotNull() &
                F.col("output_material_qty").isNotNull(),
                F.col("input_material_qty") - F.col("output_material_qty")
            ).otherwise(F.lit(None))
        ) \
        .withColumn("material_loss_pct",
            F.when(
                F.col("input_material_qty").isNotNull() &
                F.col("output_material_qty").isNotNull() &
                (F.col("input_material_qty") > 0),
                F.round(
                    (F.col("material_loss_qty") /
                     F.col("input_material_qty")) * 100, 2
                )
            ).otherwise(F.lit(None))
)

# COMMAND ----------

df_silver = df_silver.withColumn("yield_class",
        F.when(F.col("yield_percentage").isNull(),"Unknown")
         .when(F.col("yield_percentage") >= 95,"High")
         .when(F.col("yield_percentage") >= 80,"Normal")
         .otherwise("Low")
)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.withColumn("silver_processed_at", F.current_timestamp())


df_silver = df_silver.select("batch_id","lot_id","drug_code","plant_id","equipment_id","process_step","operator_id","end_timestamp","input_material_qty","output_material_qty","yield_percentage","temperature_avg","pressure_avg","status","deviation_flag","created_at","manufacture_month","manufacture_year","manufacture_quarter","shift_code","day_of_week","material_loss_qty","material_loss_pct","yield_class","silver_processed_at")

# COMMAND ----------

target_path = "s3://pfizerbucket01/processed_data/silver/mfg_lot/"

df_silver.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .save(target_path)