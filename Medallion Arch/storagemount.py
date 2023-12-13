# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "67677ed8-b30e-406e-abaf-0c19d9f44138",
"fs.azure.account.oauth2.client.secret": 'RbI8Q~6fa3ZaKSywmUo5hNmvBoX-x2fIDDB5Bc2b',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/ebcae563-3c52-4b42-beb7-f0c1fca366d4/oauth2/token"}


dbutils.fs.mount(
source = "abfss://bronze@projecttokyo.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/bronze",
extra_configs = configs)
  

# COMMAND ----------

dbutils.fs.ls("mnt/silver")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "67677ed8-b30e-406e-abaf-0c19d9f44138",
"fs.azure.account.oauth2.client.secret": 'RbI8Q~6fa3ZaKSywmUo5hNmvBoX-x2fIDDB5Bc2b',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/ebcae563-3c52-4b42-beb7-f0c1fca366d4/oauth2/token"}


dbutils.fs.mount(
source = "abfss://silver@projecttokyo.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/silver",
extra_configs = configs)
  

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "67677ed8-b30e-406e-abaf-0c19d9f44138",
"fs.azure.account.oauth2.client.secret": 'RbI8Q~6fa3ZaKSywmUo5hNmvBoX-x2fIDDB5Bc2b',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/ebcae563-3c52-4b42-beb7-f0c1fca366d4/oauth2/token"}


dbutils.fs.mount(
source = "abfss://gold@projecttokyo.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/gold",
extra_configs = configs)
  

# COMMAND ----------


