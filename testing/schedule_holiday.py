# Databricks notebook source
from pyspark.sql import functions as F

def check_date_exists(df, input_date):
    df = df.withColumn("Date", F.date_format(F.col("Date"), "yyyy-MM-dd"))
    return df.filter(F.col("Date") == input_date).count() > 0