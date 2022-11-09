# Databricks notebook source
# MAGIC %md #Imports
# MAGIC 
# MAGIC You'll need these to do your testing

# COMMAND ----------

from databricks.feature_store.client import FeatureStoreClient
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, StringType, FloatType

# COMMAND ----------

# MAGIC %md Initialize a Feature Store client

# COMMAND ----------

fs = FeatureStoreClient()

# COMMAND ----------

# MAGIC %md Set up a database for your feature tables

# COMMAND ----------

# MAGIC %sql CREATE DATABASE IF NOT EXISTS fs_repo_test

# COMMAND ----------

# MAGIC %md # Create Feature Table
# MAGIC 
# MAGIC Uses `fs.create_feature_table(...)`

# COMMAND ----------

# Create the schema (primary/partition keys and features)
schema = StructType(
  [
    # (name, type, is_null)
    StructField("id", IntegerType(), False),
    StructField("feat1", FloatType(), True),
  ]
)
# Create feature table DataFrame
df = spark.createDataFrame(
  [
    (123, 555.55),
    (456, 666.66),
  ],
  schema,
)

# Create feature table from df with name "boilerplate.table1" 
# and primary key "id" (can take a list of columns for composite keys) 
# and partition key "feat1" (can take a list of columns or be omitted)
fs.create_table("fs_repo_test.test_table", "id", df)

# Note that you can pass a schema in the `schema=` argument instead passing a DataFrame to the `features_df=` argument.
# It will create an empty feature table.

# COMMAND ----------


