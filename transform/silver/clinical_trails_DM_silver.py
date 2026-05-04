from pyspark.sql import functions as F
from delta.tables import DeltaTable

df_silver = spark.read \
    .format("delta") \
    .load("s3://pfizerbucket01/processed_data/bronze/clinical_trials_DM/")


df_silver.printSchema()
display(df_silver)
df_silver.count()

# --------------------------------------------------------
# Transformations
df_silver.select("sex").distinct().show()

df_silver = df_silver.withColumnRenamed("race", "Origin")
df_silver = df_silver.dropna(subset=["study_id","subject_id","site_id","arm_group","age","sex","Origin","country","enrollment_date","arm_group"])

display(df_silver)
df_silver.count()


df_silver = df_silver.withColumn(
    "sex",
    F.when(F.upper(F.col("sex")) == "M", "Male")
     .when(F.upper(F.col("sex")) == "F", "Female")
     .otherwise("Other")  
)

display(df_silver)

df_silver = df_silver.withColumn(
                "enrollment_date_month",
                F.date_format(F.col("enrollment_date"), "dd MMMM")
            ).withColumn(
                "enrollment_Year",
                F.date_format(F.col("enrollment_date"), "yyyy")
            ).withColumn("enrollment_quarter",
            F.concat(
                F.year("enrollment_date").cast("string"),
                F.lit("-Q"),
                F.quarter("enrollment_date").cast("string")
            )
        )

display(df_silver)

df_silver = df_silver.withColumn("hard_null_flag",
                F.when(
                        F.col("study_id").isNull()   |
                        F.col("subject_id").isNull() |
                        F.col("arm_group").isNull()  |
                        F.col("site_id").isNull(),
                        True
                    ).otherwise(False)
)


display(df_silver)

df_silver = df_silver.withColumn("hard_null_flag",
                F.when(
                        F.col("study_id").isNull()   |
                        F.col("subject_id").isNull() |
                        F.col("arm_group").isNull()  |
                        F.col("site_id").isNull(),
                        True
                    ).otherwise(False)
)


display(df_silver)

df_silver = df_silver.withColumn("age_group",
            F.when(F.col("age") < 18,"Under 18")
             .when(F.col("age").between(18, 44),"18-44")
             .when(F.col("age").between(45, 64),"45-64")
             .when(F.col("age").between(65, 74),"65-74")
             .when(F.col("age").between(75, 89),"75-89")
             .when(F.col("age") >= 90,"90+")    
             .otherwise("Unknown")
        ) \
        .drop("age")


display(df_silver)


VALID_ARMS = ["TREATMENT", "PLACEBO", "CONTROL",
                  "LOW DOSE", "HIGH DOSE", "ACTIVE COMPARATOR"]

df_silver = df_silver.withColumn("arm_group",
            F.upper(F.trim(F.col("arm_group")))
        ) \
        .withColumn("invalid_arm_flag",
            F.when(~F.col("arm_group").isin(VALID_ARMS), True).otherwise(False)
)  


internal_columns = ["hard_null_flag","invalid_arm_flag"]
df_silver = df_silver.filter(
                (F.col("hard_null_flag")==False) & (F.col("invalid_arm_flag")==False)
).drop(*internal_columns)
display(df_silver)

df_silver.printSchema()

#---------------------------------------------------------------
# Write to Silver layer
target_path = "s3://pfizerbucket01/processed_data/silver/clinical_trials_DM/"

if DeltaTable.isDeltaTable(spark, target_path):
    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("t")
        .merge(
            df_silver.alias("s"),
            "t.subject_id = s.subject_id"
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