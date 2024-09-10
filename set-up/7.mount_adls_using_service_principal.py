# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, tenant_id, and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utility mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get('formula1-scope', 'formula1-app-client-id')
tenant_id = dbutils.secrets.get('formula1-scope', 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get('formula1-scope', 'formual1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dljjcc.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dljjcc/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dljjcc/demo"))

# COMMAND ----------

df = spark.read.csv("/mnt/formula1dljjcc/demo/circuits.csv")
display(df)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dljjcc/demo')
