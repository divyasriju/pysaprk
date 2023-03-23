# Databricks notebook source

from pyspark.sql.types import StructType,IntegerType, StringType
schema=StructType().add(field='empid',data_type=IntegerType())\
                    .add(field='empname',data_type=StringType())\
                    .add(field='country',data_type=StringType())\
                    .add(field='depid',data_type=IntegerType())
df=spark.read.csv(path='https://s3.console.aws.amazon.com/s3/object/bigdatademobucket123?region=us-east-1&prefix=datasource/survey_results_public.csv
',schema=schema,header=True)
df.printSchema()
display(df)


# COMMAND ----------

df=spark.read.format('csv').load(path='dbfs:/FileStore/shared_uploads/divyasriju1803@gmail.com/emp.csv',header=True)

display(df)
df.printSchema()


# COMMAND ----------

df=spark.read.format('csv').load(path=['dbfs:/FileStore/shared_uploads/divyasriju1803@gmail.com/department.csv','dbfs:/FileStore/shared_uploads/divyasriju1803@gmail.com/emp.csv'],header=True)
display(df)
df.printSchema()


# COMMAND ----------

df=spark.read.format('csv').load(path='dbfs:/FileStore/shared_uploads/divyasriju1803@gmail.com/',header=True)
display(df)
df.printSchema()


# COMMAND ----------

