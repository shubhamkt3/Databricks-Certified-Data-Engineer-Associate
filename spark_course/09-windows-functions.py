# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('Basic') \
    .getOrCreate()

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window

# COMMAND ----------

summary_df = spark.read \
            .format('parquet') \
            .load('/mnt/bronze/Pyspark_proj/summary.parquet')

# COMMAND ----------

display(summary_df)

# COMMAND ----------

running_total_window = Window.partitionBy('Country') \
                        .orderBy('WeekNumber') 
summary_df.withColumn('RunningTotal',
                      f.count('InvoiceValue').over(running_total_window)) \
                      .show()



# COMMAND ----------


