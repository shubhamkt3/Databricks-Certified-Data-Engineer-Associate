# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

dbutils.fs.ls(f'{dataset_bookstore}/orders-raw')

# COMMAND ----------

(spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'parquet')
    .option('cloudFiles.schemaLocation', 'dbfs:/mnt/demo/orders_checkpoint')
    .load(f'{dataset_bookstore}/orders-raw')
    .writeStream
    .option('checkpointLocation','dbfs:/mnt/demo/orders-checkpoint')
    .table('orders_update')
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_update

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table orders_update

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/demo/orders_checkpoint',True)

# COMMAND ----------


