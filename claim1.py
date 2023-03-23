# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark import SparkFiles
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator as Evaluator

def vectorizeData(data, validCols, labelsCol):
    """Creates dataset from spark DataFrame getting only two columns 'label'
    and 'features'. Alist of valid column names must be provided in
    'validCols' and the original name of the labels in 'labelsCol'."""
    vectorizer = VectorAssembler(
        inputCols=validCols,
        outputCol='features'
        )

    return vectorizer.transform(data).withColumn('label',
                                                data[labelsCol].cast(IntegerType())
                                                ).select(['features', 'label'])
    
    #return StringIndexer(inputCol=labelsCol, 
    #                        outputCol="label").fit(vectorized).transform(vectorized)

spark = SparkSession \
    .builder \
    .appName("Python Spark Autoinsurance mini-example") \
    .getOrCreate()

#url = "https://raw.githubusercontent.com/jodb/sparkTestData/master/insurance_claims.csv"
#spark.sparkContext.addFile(url)
fileStore = "dbfs:/FileStore/shared_uploads/divyasriju1803@gmail.com/5xml/insurance_claim.csv"

df = spark.read.format("csv")\
          .options(inferSchema="true", header="true")\
          .load(fileStore)\
          .drop("_c39")

df = df.withColumn("policy_bind_date", df.policy_bind_date.cast("string")) \
        .withColumn("incident_date", df.incident_date.cast("string"))
label = "fraud_reported"

display(df)

# COMMAND ----------

# Remove label from list of features
stringColList = [i[0] for i in df.dtypes if i[1] == 'string' if i[0] != label]

colsToDelete = ["policy_number", "policy_bind_date", "insured_zip", "incident_location", "incident_date"]
filteredStringColList = [i for i in stringColList if i not in colsToDelete]




# COMMAND ----------

# Indexing categorical features
indexedCategoricalCols = [categoricalCol + "Index" for categoricalCol in filteredStringColList]
stages = [StringIndexer(inputCol = categoricalCol, outputCol = idx_categoricalCol) 
                                for categoricalCol, idx_categoricalCol in zip(filteredStringColList, 
                                                                                indexedCategoricalCols)]
indexer = Pipeline(stages=stages)
df = indexer.fit(df).transform(df)
