from pyspark.sql import functions as F
from delta.tables import DeltaTable

# secret file 
base_path = dbutils.secrets.get("bucket","clinical_trails_LB")

print("base_path",base_path)

# read the source data
df_bronze = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("pathGlobFilter", "*.csv")
    .option("header","true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "s3://pfizerbucket01/processed_data/bronze/autoloader/6/")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(base_path)
)


# write the data to delta lake
from pyspark.sql.functions import col
(
    df_bronze
    .withColumn("__file",col("_metadata.file_name"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", "s3://pfizerbucket01/processed_data/bronze/autoloader/6/")
    .outputMode("append")
    .trigger(availableNow=True)
    .start("s3://pfizerbucket01/processed_data/bronze/clinical_trials_LB/")
)