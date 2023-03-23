# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location1 = "/FileStore/tables/matches.csv"
file_location2 = "/FileStore/tables/deliveries.csv"


file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df1 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location1)
df2 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location2)

display(df1)
display(df2)

# COMMAND ----------

# Create a view or table

temp_table_name1 = "matches_csv"
temp_table_name2="deliveries_csv"

df1.createOrReplaceTempView(temp_table_name1)
df2.createOrReplaceTempView(temp_table_name2)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `matches_csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `deliveries_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "matches_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------


 

# COMMAND ----------

# DBTITLE 1, 
# MAGIC %sql
# MAGIC 
# MAGIC select * from matches_csv
# MAGIC where season = 2017 and ((win_by_runs >50) or (win_by_wickets >5 ))

# COMMAND ----------

df1.columns

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC select  Player_of_match from matches_csv where venue= "Eden Gardens" 

# COMMAND ----------




where season =2017



# COMMAND ----------


where venue= "Wankhede Stadium"


# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC     

# COMMAND ----------

df2.columns
