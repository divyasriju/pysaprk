# Databricks notebook source
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
               .appName('SparkByExamples.com') \
               .getOrCreate()
data=[["1"]]
df=spark.createDataFrame(data,["id"])

from pyspark.sql.functions import *
#current_date() & current_timestamp()
df.withColumn("current_date",current_date()) \
  .withColumn("current_timestamp",current_timestamp()) \
  .show(truncate=False)


# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#from pyspark.sql.types import *
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#schema = StructType([
         #StructField("id", StringType(), True)])

schema=["id","fullname","lastname"]
dates =[[1,"sreya","divya"]]

df = spark.createDataFrame(data=dates, schema=schema)

df.withColumn("current_date",current_date()) \
  .withColumn("current_timestamp",current_timestamp()) \
  .show(truncate=False)


df.printSchema()

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

from pyspark.sql.functions import col,expr
data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)]
spark.createDataFrame(data).toDF("date","increment") \
    .select(col("date"),col("increment"), \
      expr("add_months(to_date(date,'yyyy-MM-dd'),cast(increment as int))").alias("inc_date")) \
    .show()


# COMMAND ----------

help(expr)
help(to_date)
help(add_months)

# COMMAND ----------

