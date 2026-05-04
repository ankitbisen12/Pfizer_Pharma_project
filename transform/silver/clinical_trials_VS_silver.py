# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

df_silver = spark.read \
    .format("delta") \
    .load("s3://pfizerbucket01/processed_data/bronze/clinical_trials_VS/")


# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

display(df_silver)

df_silver.count()

# COMMAND ----------

df_silver = df_silver.dropna(subset=["study_id","subject_id","visit_date","heart_rate","systolic_bp","diastolic_bp","temperature","respiratory_rate"])


# COMMAND ----------

df_silver = df_silver.withColumn(
                        "visit_date_month",
                        F.date_format(F.col("visit_date"), "dd MMMM")
                      ).withColumn(
                        "visit_Year",
                        F.date_format(F.col("visit_date"), "yyyy")
                      )

display(df_silver)


# COMMAND ----------

df_silver = df_silver.withColumn("heart_rate_category",
        F.when(F.col("heart_rate").isNull(),"Unknown")
         .when(F.col("heart_rate")< 40,"Severe Bradycardia")
         .when(F.col("heart_rate")< 60,"Bradycardia")
         .when(F.col("heart_rate")<= 100,"Normal")
         .when(F.col("heart_rate")<= 150,"Tachycardia")
         .otherwise("Severe Tachycardia")
    )

# COMMAND ----------

df_silver = df_silver.withColumn("bp_category",
        F.when(
            F.col("systolic_bp").isNull() | F.col("diastolic_bp").isNull(),
            "Unknown"
        )
        .when(
            (F.col("systolic_bp") < 70) | (F.col("diastolic_bp") < 40),
            "Hypotensive"
        )
        .when(
            (F.col("systolic_bp") < 120) & (F.col("diastolic_bp") < 80),
            "Normal"
        )
        .when(
            (F.col("systolic_bp") < 130) & (F.col("diastolic_bp") < 80),
            "Elevated"
        )
        .when(
            (F.col("systolic_bp") < 140) | (F.col("diastolic_bp") < 90),
            "HTN Stage 1"
        )
        .when(
            (F.col("systolic_bp") >= 140) | (F.col("diastolic_bp") >= 90),
            "HTN Stage 2"
        )
        .otherwise("HTN Crisis")
    )

# COMMAND ----------

 df_silver = df_silver.withColumn("temp_category",
        F.when(F.col("temperature").isNull(),"Unknown")
         .when(F.col("temperature")< 35.0, "Hypothermia")
         .when(F.col("temperature")<= 37.2,"Normal")
         .when(F.col("temperature")<= 38.0,"Low Grade Fever")
         .when(F.col("temperature")<= 40.0,"High Fever")
         .otherwise("Hyperpyrexia")
    )

# COMMAND ----------

# Respiratory rate category
df_silver = df_silver.withColumn("rr_category",
        F.when(F.col("respiratory_rate").isNull(),"Unknown")
         .when(F.col("respiratory_rate")< 8,"Severe Bradypnea")
         .when(F.col("respiratory_rate")< 12,"Bradypnea")
         .when(F.col("respiratory_rate")<= 20,"Normal")
         .when(F.col("respiratory_rate")<= 30,"Tachypnea")
         .otherwise("Severe Tachypnea")
    )

# COMMAND ----------

df_silver = df_silver.withColumn(
    "silver_processed_at",
    F.current_timestamp()
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = df_silver.select("study_id","subject_id","heart_rate","systolic_bp","visit_date","diastolic_bp","temperature", "respiratory_rate","visit_date_month","visit_Year","temp_category","rr_category","bp_category","heart_rate_category","silver_processed_at")



# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

target_path = "s3://pfizerbucket01/processed_data/silver/clinical_trials_VS/"

df_silver.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("append") \
    .save(target_path)