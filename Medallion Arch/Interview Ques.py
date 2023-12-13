# Databricks notebook source

users_data = [("A", "B"),
              ("A", "C"),
              ("A", "D"),
              ("A", "F"),
              ("B", "C"),
              ("B", "D"),
              ("B", "E")
              ]

application_data = [
("A","Facebook"),
("B","Linkedin"),
("B","Twitter"),
("C","Instagram"),
("C","Twitter"),
("D","Youtube"),
("E","Snapchat"),
("F","Netflix"),
("F","Facebook")]

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType

user_schema = StructType([StructField('user_name', StringType(), True),StructField('friend_name', StringType(), True)])

application_schema = StructType([StructField('user_name', StringType(), True),StructField('application', StringType(), True)])
user_df = spark.createDataFrame(users_data, user_schema)
app_df = spark.createDataFrame(application_data, application_schema)

# COMMAND ----------

user_df.createOrReplaceTempView('users')
app_df.createOrReplaceTempView('app')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct a.user_name, application 
# MAGIC from users a
# MAGIC join app b on a.friend_name = b.user_name
# MAGIC where application not in (SELECT distinct c.application
# MAGIC FROM app c where a.user_name = c.user_name)
# MAGIC --group by a.user_name
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

user_df.alias('a').join(app_df.alias('b'), f.col('a.friend_name') == f.col('b.user_name')) \
    .join(app_df.alias('c'), (f.col('a.user_name')== f.col('c.user_name')) & (f.col('b.application')==f.col('c.application')), 'left_outer') \
    .where(f.col('c.application').isNull()) \
    .select(f.col('a.user_name'), f.col('b.application')) \
    .distinct() \
    .orderBy('a.user_name') \
    .show()

# COMMAND ----------


