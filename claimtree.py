# Databricks notebook source
import numpy as np #used to perform mathematical functions on multi-dimensional arrays and matrices
from pyspark.ml.feature import StringIndexer, OneHotEncoder # label indexer that maps a string column of labels to an 
#ML column of label indices.
from pyspark.ml.feature import VectorAssembler #feature transformer that merges multiple columns into a vector column.
from pyspark.ml.feature import MinMaxScaler, StandardScaler #Rescale each feature individually to a common range [min, max] 
#linearly using column summary statistics,
from pyspark.ml import Pipeline #Pipeline consists of a sequence of stages, each of which is either 
#an Estimator or a Transformer


#ML Linear Regression
from pyspark.ml.regression import LinearRegression
#Ml Decision tree regressor
from pyspark.ml.regression import DecisionTreeRegressor
#ML Random Forest regressor
from pyspark.ml.regression import RandomForestRegressor

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator #Builder for a param grid used in grid search-based model selection.

import time

# COMMAND ----------

#Reading the dataframe of insurance 
df=spark.read.csv(path="dbfs:/FileStore/shared_uploads/divyasriju1803@gmail.com/insurance_claims.csv", sep=",", header=True, inferSchema=True)
display(df)

# COMMAND ----------

