# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

df_lot = spark.read.format("delta") \
        .load("s3://pfizerbucket01/processed_data/silver/mfg_lot/")

df_orders = spark.read.format("delta") \
        .load("s3://pfizerbucket01/processed_data/silver/mfg_order/")

df_equip = spark.read.format("delta") \
        .load("s3://pfizerbucket01/processed_data/silver/mfg_equipment_log/")

df_dev = spark.read.format("delta") \
        .load("s3://pfizerbucket01/processed_data/silver/mfg_deviation/")

# COMMAND ----------

lot_agg = df_lot.groupBy(
            "plant_id", "drug_code", "manufacture_month",
            "manufacture_year", "manufacture_quarter"
        ).agg(
            F.count("batch_id").alias("total_batches"),
            F.sum(F.when(F.col("status") == "COMPLETED", 1)
                   .otherwise(0)).alias("completed_batches"),
            F.sum(F.when(F.col("status") == "FAILED", 1)
                   .otherwise(0)).alias("failed_batches"),
            F.sum(F.when(F.col("status") == "IN_PROGRESS", 1)
                   .otherwise(0)).alias("inprogress_batches"),
            F.round(F.avg("yield_percentage"), 2).alias("avg_yield_pct"),
            F.round(F.min("yield_percentage"), 2).alias("min_yield_pct"),
            F.round(F.max("yield_percentage"), 2).alias("max_yield_pct"),
            F.sum(F.when(F.col("yield_class") == "High", 1)
                   .otherwise(0)).alias("high_yield_batches"),
            F.sum(F.when(F.col("yield_class") == "Low", 1)
                   .otherwise(0)).alias("low_yield_batches"),
            F.sum(F.when(F.col("deviation_flag") == True, 1)
                   .otherwise(0)).alias("deviation_batches"),
            F.round(F.avg("batch_duration_minutes"), 1).alias("avg_batch_duration_mins"),
            F.round(F.avg("material_loss_pct"), 2).alias("avg_material_loss_pct"),

            F.round(F.sum("input_material_qty"), 2).alias("total_input_qty"),
            F.round(F.sum("output_material_qty"), 2).alias("total_output_qty")
        ) \
        .withColumn("batch_pass_rate_pct",
            F.round(
                (F.col("completed_batches") / F.col("total_batches")) * 100, 2
            )
        ) \
        .withColumn("batch_fail_rate_pct",
            F.round(
                (F.col("failed_batches") / F.col("total_batches")) * 100, 2
            )
        )

# COMMAND ----------

orders_agg = df_orders.groupBy("drug_code", "date_month") \
        .agg(
            F.count("order_id")                                         .alias("total_orders"),

            F.sum(F.when(F.col("status") == "DONE", 1)
                   .otherwise(0))                                       .alias("completed_orders"),

            F.round(F.avg("qty_variance_pct"), 2)                      .alias("avg_qty_variance_pct"),

            F.sum(F.when(
                F.col("fulfillment_status") == "On-Target", 1)
                .otherwise(0))                                          .alias("on_target_orders"),

            F.sum(F.when(
                F.col("fulfillment_status") == "Under", 1)
                .otherwise(0))                                          .alias("under_orders"),

            F.round(F.avg("planned_duration_days"), 1)                 .alias("avg_planned_duration_days")
        ) \
        .withColumn("qty_fulfillment_rate_pct",
            F.round(
                (F.col("on_target_orders") / F.col("total_orders")) * 100, 2
            )
        ) \
        .withColumn("on_time_orders_pct",
            F.round(
                (F.col("completed_orders") / F.col("total_orders")) * 100, 2
            )
        )

        

# COMMAND ----------

lot_keys = df_lot.select(
        "batch_id", "plant_id", "drug_code",
        "manufacture_month", "yield_class"
    ).distinct()

df_dev_enriched = df_dev.join(lot_keys, on="batch_id", how="left")

df_dev_enriched = df_dev_enriched.withColumn("reported_month",
        F.date_format("reported_date", "yyyy-MM")
    )

gold_capa = df_dev_enriched.groupBy(
            "plant_id", "drug_code", "reported_month"
        ).agg(
            F.count("deviation_id").alias("total_deviations"),

            # Status breakdown
            F.sum(F.when(F.col("status") == "OPEN", 1)
                   .otherwise(0)).alias("open_count"),
            F.sum(F.when(F.col("status") == "CLOSED", 1)
                   .otherwise(0)).alias("closed_count"),
            F.sum(F.when(F.col("status") == "IN_REVIEW", 1)
                   .otherwise(0)).alias("in_review_count"),

            # Severity breakdown
            F.sum(F.when(F.col("severity") == "CRITICAL", 1)
                   .otherwise(0)).alias("critical_count"),
            F.sum(F.when(F.col("severity") == "MAJOR", 1)
                   .otherwise(0)).alias("major_count"),
            F.sum(F.when(F.col("severity") == "MINOR", 1)
                   .otherwise(0)).alias("minor_count"),

            # Critical OPEN — highest regulatory risk
            F.sum(F.when(
                (F.col("severity") == "CRITICAL") &
                (F.col("status")   != "CLOSED"),
                1).otherwise(0)).alias("critical_open_count"),

            # CAPA timeline metrics
            F.round(F.avg("days_to_closure"), 1).alias("avg_days_to_closure"),
            F.round(F.min("days_to_closure"), 0).alias("min_days_to_closure"),
            F.round(F.max("days_to_closure"), 0).alias("max_days_to_closure"),

            # SLA breach — days_to_closure > sla_days
            F.sum(F.when(
                F.col("days_to_closure").isNotNull() &
                (F.col("days_to_closure") > F.col("sla_days")),
                1).otherwise(0)).alias("sla_breach_count"),

            # Deviation type breakdown
            F.sum(F.when(F.col("deviation_type") == "PROCESS", 1)
                   .otherwise(0)).alias("process_deviations"),
            F.sum(F.when(F.col("deviation_type") == "EQUIPMENT", 1)
                   .otherwise(0)).alias("equipment_deviations"),
            F.sum(F.when(F.col("deviation_type") == "MATERIAL", 1)
                   .otherwise(0)).alias("material_deviations"),
            F.countDistinct("batch_id").alias("affected_batches"),
            F.countDistinct("equipment_id").alias("affected_equipment")
        ) \
        .withColumn("sla_breach_rate_pct",
            F.round(
                (F.col("sla_breach_count") / F.col("total_deviations")) * 100, 2
            )
        ) \
        .withColumn("closure_rate_pct",
            F.round(
                (F.col("closed_count") / F.col("total_deviations")) * 100, 2
            )
        ) \
        .withColumn("capa_risk_level",
            # Site-level risk signal for QA director dashboard
            F.when(F.col("critical_open_count") > 0,  "Critical")
             .when(F.col("sla_breach_rate_pct")  > 30, "High")
             .when(F.col("sla_breach_rate_pct")  > 15, "Medium")
             .otherwise(                               "Low")
        ) \
        .withColumn("gold_processed_at", F.current_timestamp())


display(gold_capa)

# COMMAND ----------

gold_capa.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://pfizerbucket01/processed_data/gold/gold_capa/")