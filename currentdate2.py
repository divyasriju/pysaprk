# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import current_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

schema = StructType([
            StructField("seq", StringType(), True)])

dates = [['1']]

df = spark.createDataFrame(data=dates, schema=schema)

df.withColumn("current_date",current_date())\
    .withColumn("current_timestamp",current_timestamp()).show(truncate=False)



# COMMAND ----------

