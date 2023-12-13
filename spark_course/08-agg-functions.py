# Databricks notebook source
dbutils.fs.ls('/mnt/bronze/Pyspark_proj')

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName('Basic') \
        .getOrCreate()

# COMMAND ----------

df = spark.read \
        .format('csv') \
        .option('header', 'true') \
        .load('/mnt/bronze/Pyspark_proj/invoices.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import types as t
df.select(f.count('*').alias('count'),
          f.sum('Quantity').alias('sum'),
          f.avg('UnitPrice').alias('avg'),
          (f.countDistinct('CustomerID').alias('Count Distinct'))) \
    .show()

# COMMAND ----------

df.selectExpr('Count(*) as count',
              'sum(Quantity) as sum') \
              .show()

# COMMAND ----------

df.groupBy('Country','InvoiceNo') \
    .agg(f.sum('Quantity').alias('TotalQuantity'),
         f.round(f.sum(f.expr('Quantity*UnitPrice')),2).alias('InvoiceValue')) \
.show()

# COMMAND ----------

new_df = df.withColumn('WeekNumber', f.weekofyear(f.to_date(f.col('InvoiceDate'),'dd-MM-yyyy h.mm')))
#new_df = df.withColumn('InvoiceDate', f.to_date('InvoiceDate','dd-MM-yyyy h.mm'))
new_df.show()

# COMMAND ----------

new_df = new_df.withColumn('weekOfYear', f.weekofyear('InvoiceDate'))
new_df.show(5)

# COMMAND ----------

new_df.groupBy('Country','WeekOfYear') \
    .agg(f.countDistinct('InvoiceNo').alias('NumInvoices'),
         f.sum('Quantity').alias('TotalQuantity'),
         f.sum(f.expr('Quantity*UnitPrice')).alias('InvoiceValue')) \
             .orderBy('WeekOfYear', ascending=False) \
    .show(40)         

# COMMAND ----------

new_df.createOrReplaceTempView('invoice')

# COMMAND ----------

spark.sql('Select Country, weekofyear, count(distinct InvoiceNo) as NumInvoices from invoice \
          where country = "EIRE" group by Country, weekofyear') \
.show()

# COMMAND ----------


