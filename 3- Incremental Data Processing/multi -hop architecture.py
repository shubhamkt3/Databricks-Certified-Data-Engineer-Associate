# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f'{dataset_bookstore}/orders-raw')
display(files)

# COMMAND ----------

(spark.readStream
    .format('CloudFiles')
    .option('CloudFiles.format', 'parquet')
    .option('CloudFiles.schemaLocation','dbfs:/mnt/demo/checkpoint/orders_raw')
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView('orders_raw_temp')
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create or replace temp view orders_tmp as (
# MAGIC   Select *, current_timestamp() as arrival_time, input_file_name()
# MAGIC   from orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_tmp

# COMMAND ----------

(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
      .outputMode("append")
      .table("orders_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------


