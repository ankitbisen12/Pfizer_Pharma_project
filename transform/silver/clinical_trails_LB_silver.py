from pyspark.sql import functions as F
from delta.tables import DeltaTable

df_silver = spark.read \
    .format("delta") \
    .load("s3://pfizerbucket01/processed_data/bronze/clinical_trials_LB/")

df_silver.printSchema()

display(df_silver)

df_silver.count()

#----------------------------------------------------------------
# Transformtaions

df_silver = df_silver.dropna(subset=["study_id","subject_id","test_name","test_value","unit","reference_range","test_date","abnormal_flag"])


df_silver = df_silver.withColumn("abnormal_flag", F.upper(F.col("abnormal_flag")))\
                     .withColumn(
                        "test_date_month",
                        F.date_format(F.col("test_date"), "dd MMMM")
                      ).withColumn(
                        "test_Year",
                        F.date_format(F.col("test_date"), "yyyy")
                      )

display(df_silver)


df_silver = df_silver.withColumn(
    "lower_range",
    F.split(F.col("reference_range"), "-").getItem(0).cast("double")
).withColumn(
    "upper_range",
    F.split(F.col("reference_range"), "-").getItem(1).cast("double")
)

df_silver = df_silver.withColumn(
    "derived_abnormal_flag",
    F.when(F.col("test_value") < F.col("lower_range"), "LOW")
     .when(F.col("test_value") > F.col("upper_range"), "HIGH")
     .otherwise("NORMAL")
)

df_silver = df_silver.withColumn(
    "abnormal_mismatch_flag",
    F.when(
        F.col("abnormal_flag") != F.col("derived_abnormal_flag"),
        True
    ).otherwise(False)
)
display(df_silver)

df_silver = df_silver.withColumn(
    "silver_processed_at",
    F.current_timestamp()
)

df_silver = df_silver.filter(F.col("abnormal_mismatch_flag")==False)

df_silver = df_silver.select("study_id","subject_id","test_name","test_value","test_date","unit","lower_range","upper_range","abnormal_flag","test_date_month","test_Year")


target_path = "s3://pfizerbucket01/processed_data/silver/clinical_trials_LB/"

df_silver.write \
        .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .mode("overwrite") \
        .save(target_path)