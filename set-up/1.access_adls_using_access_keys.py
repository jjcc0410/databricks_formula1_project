# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access key
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List the files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key =  dbutils.secrets.get('formula1-scope', 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dljjcc.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dljjcc.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.csv("abfss://demo@formula1dljjcc.dfs.core.windows.net/circuits.csv")
display(df)
