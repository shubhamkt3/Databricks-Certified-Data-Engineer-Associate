# Databricks notebook source
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from pyspark.sql import Row
from pyspark.sql.functions import to_date, col

# COMMAND ----------

schema = StructType([StructField('Id', StringType()), StructField('EventDate', StringType())])

data = [Row('1','01/11/2023'),Row('2','02/11/2023'),Row('3','03/11/2023'),Row('4','04/11/2023')]
df = spark.createDataFrame(data, schema)

# COMMAND ----------

def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(col(fld), fmt))

# COMMAND ----------

updated_df = to_date_df(df, 'dd/MM/yyyy', 'EventDate')

# COMMAND ----------

updated_df.printSchema()

# COMMAND ----------

rows = updated_df.collect()

# COMMAND ----------

for r in rows:
    print(type(r['Id']))

# COMMAND ----------


