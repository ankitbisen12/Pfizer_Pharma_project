# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer Processing

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

df_silver = spark.read \
    .format("delta") \
    .load("s3://pfizerbucket01/processed_data/bronze/QC_Lab_result/")

df_silver.count()

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.withColumn("result_flag",F.upper(F.col("result_flag")))\
                     .withColumn("test_name",F.upper(F.col("test_name")))

display(df_silver)

# COMMAND ----------

VALID_TEST_NAME = ["PH","STERILITY","POTENCY","ENDOTOXIN"]
df_Silver = df_silver.withColumn("test_name",
                F.when(
                    F.col("test_name").isin(VALID_TEST_NAME),
                    F.col("test_name")
                ).otherwise("UNKNOWN")           
)

# COMMAND ----------

# 
df_silver = df_silver.withColumn("hard_null_flag",
        F.when(
            F.col("batch_id").isNull()  |
            F.col("lot_id").isNull()    |
            F.col("test_id").isNull()   |
            F.col("test_name").isNull(),
            True
        ).otherwise(False)
    )

df_silver.select("hard_null_flag").distinct().show()


# COMMAND ----------

df_silver = df_silver.withColumn("analyst_id",
            F.when(F.col("analyst_id").isNull(), F.lit("UNKNOWN"))
             .otherwise(F.col("analyst_id"))
        ) \
        .withColumn("result_flag",
            F.when(F.col("result_flag").isNull(), F.lit("PENDING"))
             .otherwise(F.col("result_flag"))
        )

# COMMAND ----------

df_silver = df_silver.withColumn("unit",
                F.trim(F.col("unit"))                
)

# COMMAND ----------

df_silver = df_silver.withColumn("test_value_null_flag",
            F.when(F.col("test_value").isNull(), True).otherwise(False)
        ) \
        .withColumn("spec_null_flag",
            F.when(
                F.col("lower_spec").isNull() | F.col("upper_spec").isNull(),
                True
            ).otherwise(False)
        ) \
        .withColumn("unit_null_flag",
            F.when(F.col("unit").isNull(), True).otherwise(False)
)


# COMMAND ----------

df_silver = df_silver.withColumn("test_year",       F.year("test_date").cast("string")) \
           .withColumn("date_month",      F.date_format("test_date", "dd MMMM")) \
           .withColumn("test_quarter",
               F.concat(
                   F.year("test_date").cast("string"),
                   F.lit("-Q"),
                   F.quarter("test_date").cast("string")
               )
           )

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("silver_processed_at", F.current_timestamp())

# COMMAND ----------

df_silver.filter(
        (F.col("hard_null_flag")==False)|
        (F.col("test_value_null_flag")==False)|
        (F.col("unit_null_flag")==False)|
        (F.col("spec_null_flag")==False)
        )

# COMMAND ----------

df_silver = df_silver.select("batch_id","lot_id","test_id","test_name","test_value","unit","lower_spec","upper_spec","result_flag","analyst_id","test_year","date_month","test_quarter","silver_processed_at")

display(df_silver)

# COMMAND ----------

target_path = "s3://pfizerbucket01/processed_data/silver/QC_Lab_result/"

if DeltaTable.isDeltaTable(spark, target_path):
    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("t")
        .merge(
            df_silver.alias("s"),
            "t.lot_id = s.lot_id AND t.test_id = s.test_id"
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

# COMMAND ----------

df_silver.printSchema()