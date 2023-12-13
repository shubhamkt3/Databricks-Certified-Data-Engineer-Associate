# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('Basic') \
        .getOrCreate()

# COMMAND ----------

data_list = [('Ravi',28,1,'2002'),
             ('Abdul',23,5,81),
             ('John',12,12,6),
             ('Rosy',7,8,63),
             ('Abdul',23,5,81)]
raw_df = spark.createDataFrame(data_list).toDF('Name','day','month','year').repartition(3)
raw_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, expr, col, when, to_date
df1 = raw_df.withColumn('id', monotonically_increasing_id())
df1.show()

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType
df1 = df1.withColumn('year', expr("""case when year<=23 THEN year+2000
                                  WHEN year <=100 THEN year+1900
                                  ELSE year
                                  End""").cast(IntegerType()))
df1.show()

# COMMAND ----------

df2= df1.withColumn('year', df1.year.cast(StringType()))
df2.show()

# COMMAND ----------

raw_df.printSchema()

# COMMAND ----------

df6 = raw_df.withColumn('year', \
                    when(col('year')<23, col('year')+2000) \
                    .when(col('year')<100, col('year')+1900) \
                    .otherwise(col('year')) \
                        .cast(IntegerType()))

# COMMAND ----------

df6.show()

# COMMAND ----------

df8 = df6.withColumn('dob', to_date(expr('concat(day,"/",month,"/",year)'), 'd/M/yyyy')) 


# COMMAND ----------

df8 = df8.drop('day','month','year') \
    .dropDuplicates(['name','dob'])
df8.show()

# COMMAND ----------

df8.sort('dob', ascending=False) \
    .show()

# COMMAND ----------

kd
