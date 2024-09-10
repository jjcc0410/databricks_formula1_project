# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access key
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 2. List the files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dljjcc.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.csv("abfss://demo@formula1dljjcc.dfs.core.windows.net/circuits.csv")
display(df)
