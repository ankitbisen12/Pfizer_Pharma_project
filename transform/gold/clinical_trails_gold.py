from pyspark.sql import functions as F

# Raeding silver tables for DM, LB, VS
dm_gold= spark.read.format("delta") \
        .load("s3://pfizerbucket01/processed_data/silver/clinical_trials_DM/")

lb_gold = spark.read.format("delta") \
        .load("s3://pfizerbucket01/processed_data/silver/clinical_trials_LB/")

vs_gold = spark.read.format("delta") \
        .load("s3://pfizerbucket01/processed_data/silver/clinical_trials_VS/")



# Performing joins for enrichment and then aggregations to create gold tables for subject-level safety summary and demographic/safety summary
lb_enriched = lb_gold.join(
        dm_gold.select("subject_id", "study_id", "arm_group",
                  "age_group", "sex", "country",
                  "site_id", "enrollment_date_month"),
        on=["subject_id", "study_id"],
        how="left"
)

vs_enriched = vs_gold.join(
        dm_gold.select("subject_id", "study_id", "arm_group",
                  "age_group", "sex", "country", "site_id"),
        on=["subject_id", "study_id"],
        how="left"
)


# Aggregating lab data to subject level summaries
lb_per_subject = lb_enriched.groupBy("subject_id", "study_id") \
        .agg(
            F.count("test_name").alias("total_lab_tests"),
            F.sum(F.when(F.col("abnormal_flag") != "N", 1)
                   .otherwise(0)).alias("abnormal_lab_count"),
            F.round(F.avg("test_value"),2).alias("avg_lab_value"),
            F.countDistinct("test_name").alias("distinct_tests"),
            F.countDistinct("test_date_month").alias("lab_visit_months")
        ) \
        .withColumn("abnormal_lab_rate_pct",
            F.round(
                (F.col("abnormal_lab_count") / F.col("total_lab_tests")) * 100, 2
            )
        )

display(lb_per_subject)


vs_per_subject = vs_enriched.groupBy("subject_id", "study_id") \
        .agg(
            F.count("visit_date_month").alias("total_vs_visits"),
            F.sum(F.when(
                    (F.col("heart_rate_category")!= "Normal") |
                    (F.col("bp_category")!= "Normal") |
                    (F.col("temp_category")!= "Normal") |
                    (F.col("rr_category")!= "Normal"),
                    1).otherwise(0)).alias("concern_visit_count"),
            F.round(F.avg("heart_rate"),2).alias("avg_heart_rate"),
            F.round(F.avg("systolic_bp"),2).alias("avg_systolic_bp"),
            F.round(F.avg("diastolic_bp"),2).alias("avg_diastolic_bp"),
            F.round(F.avg("temperature"),2).alias("avg_temperature")
        ) \
        .withColumn("vs_concern_rate_pct",
            F.round(
                (F.col("concern_visit_count") / F.col("total_vs_visits")) * 100, 2
            )
        )

display(vs_per_subject)


gold_subject = dm_gold.join(lb_per_subject, on=["subject_id","study_id"], how="left") \
                     .join(vs_per_subject, on=["subject_id","study_id"], how="left") \
        .select(
            "study_id", "subject_id", "arm_group",
            "sex", "age_group", "country", "site_id",
            "enrollment_date_month", "enrollment_Year",
            # Lab summary
            "total_lab_tests", "abnormal_lab_count",
            "abnormal_lab_rate_pct", "distinct_tests",
            # VS summary
            "total_vs_visits", "concern_visit_count",
            "vs_concern_rate_pct", "avg_heart_rate",
            "avg_systolic_bp", "avg_diastolic_bp", "avg_temperature"
        ) \
        .withColumn("overall_safety_flag",
            # Subject flagged if >20% lab abnormal OR >30% VS concern visits
            F.when(
                (F.col("abnormal_lab_rate_pct") > 20) |
                (F.col("vs_concern_rate_pct")   > 30),
                True
            ).otherwise(False)
        ) \
        .withColumn("gold_processed_at", F.current_timestamp())

display(gold_subject)


subject_safety = dm_gold \
        .join(lb_per_subject, on=["subject_id","study_id"], how="left") \
        .join(vs_per_subject, on=["subject_id","study_id"], how="left")

gold_demo_safety = subject_safety.groupBy(
            "study_id", "arm_group", "sex", "age_group", "country"
        ).agg(
            F.countDistinct("subject_id").alias("subject_count"),
            F.round(F.avg("abnormal_lab_rate_pct"),2).alias("avg_abnormal_lab_rate_pct"),
            F.round(F.avg("vs_concern_rate_pct"),2).alias("avg_vs_concern_rate_pct"),
            F.round(F.avg("total_lab_tests"),1).alias("avg_lab_tests_per_subject"),
            F.round(F.avg("total_vs_visits"),1).alias("avg_vs_visits_per_subject"),
            F.sum(F.when(
                (F.col("abnormal_lab_rate_pct")> 20) |
                (F.col("vs_concern_rate_pct")> 30), 1)
                .otherwise(0)).alias("flagged_subjects")
        ) \
        .withColumn("flagged_subject_rate_pct",
            F.round(
                (F.col("flagged_subjects") / F.col("subject_count")) * 100, 2
            )
        ) \
        .withColumn("gold_processed_at", F.current_timestamp())

display(gold_demo_safety)



# Writing gold tables back to S3 in Delta format, partitioned by study_id for efficient querying and future updates
gold_subject.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://pfizerbucket01/processed_data/gold/clinical-trails/gold_subject/")

gold_demo_safety.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://pfizerbucket01/processed_data/gold/clinical-trails/gold_demo_safety/")