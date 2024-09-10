# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign role 'Storage Blob Container Contributor' to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get('formula1-scope', 'formula1-app-client-id')
tenant_id = dbutils.secrets.get('formula1-scope', 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get('formula1-scope', 'formual1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dljjcc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dljjcc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dljjcc.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dljjcc.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dljjcc.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dljjcc.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.csv("abfss://demo@formula1dljjcc.dfs.core.windows.net/circuits.csv")
display(df)
