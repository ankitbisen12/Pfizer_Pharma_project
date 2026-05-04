from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Reading silver table for QC_Lab_result
df_silver = spark.read \
    .format("delta") \
    .load("s3://pfizerbucket01/processed_data/silver/QC_Lab_result/")


display(df_silver)

# Aggregating data to create gold tables for overall QC summary, trend analysis, batch quality, and analyst performance
df_gold = (
    df_silver
    .groupBy("test_name")
    .agg(
        F.count("*").alias("total_tests"),
        F.sum(F.when(F.col("result_flag") == "PASS", 1).otherwise(0)).alias("pass_count"),
        F.sum(F.when(F.col("result_flag") == "FAIL", 1).otherwise(0)).alias("fail_count")
    )
    .withColumn(
        "pass_pct",
        F.round((F.col("pass_count") / F.col("total_tests")) * 100, 2)
    )
    .withColumn(
        "fail_pct",
        F.round((F.col("fail_count") / F.col("total_tests")) * 100, 2)
    )
)


df_trend = (
    df_silver
    .groupBy("test_name", "date_month")
    .agg(
        F.count("*").alias("total_tests"),
        F.sum(F.when(F.col("result_flag") == "FAIL", 1).otherwise(0)).alias("fail_count")
    )
    .withColumn(
        "fail_pct",
        F.round((F.col("fail_count") / F.col("total_tests")) * 100, 2)
    )
)

df_batch_quality = (
    df_silver
    .groupBy("batch_id")
    .agg(
        F.count("*").alias("total_tests"),
        F.sum(F.when(F.col("result_flag") == "FAIL", 1).otherwise(0)).alias("fail_count")
    )
    .withColumn(
        "batch_quality_score",
        F.round(100 - (F.col("fail_count") / F.col("total_tests") * 100), 2)
    )
)
display(df_batch_quality)


df_analyst = (
    df_silver
    .groupBy("analyst_id")
    .agg(
        F.count("*").alias("tests_performed"),
        F.sum(F.when(F.col("result_flag") == "FAIL", 1).otherwise(0)).alias("fail_count"),
        F.sum(F.when(F.col("result_flag") == "PASS", 1).otherwise(0)).alias("pass_count")
    )
    .withColumn(
        "fail_pct",
        F.round((F.col("fail_count") / F.col("tests_performed")) * 100, 2)
    )
    .withColumn(
        "pass_pct",
        F.round((F.col("pass_count") / F.col("tests_performed")) * 100, 2)
    )
)

display(df_analyst)


# Writing gold tables back to S3 in Delta format
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://pfizerbucket01/processed_data/gold/QC_Lab_result/qc_summary/")

df_trend.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://pfizerbucket01/processed_data/gold/QC_Lab_result/qc_trend/")

df_batch_quality.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://pfizerbucket01/processed_data/gold/QC_Lab_result/qc_batch_quality/")

df_analyst.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://pfizerbucket01/processed_data/gold/QC_Lab_result/qc_analyst/")
