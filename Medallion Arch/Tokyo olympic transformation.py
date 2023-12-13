# Databricks notebook source
from pyspark.sql.functions import col
from  pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "67677ed8-b30e-406e-abaf-0c19d9f44138",
"fs.azure.account.oauth2.client.secret": 'RbI8Q~6fa3ZaKSywmUo5hNmvBoX-x2fIDDB5Bc2b',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/ebcae563-3c52-4b42-beb7-f0c1fca366d4/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-date@projecttokyo.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyo",
extra_configs = configs)
  

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyo"

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pyspark").getOrCreate()

# COMMAND ----------

dataSchema = [StructField('Discipline', StringType(), True), StructField('Female', IntegerType(), True),StructField('Male', IntegerType(), True), StructField('Total', IntegerType(), True)]
final_struct = StructType(fields = dataSchema)

# COMMAND ----------

athletes = spark.read.format('csv').option('header','true').load('/mnt/tokyo/raw-data/Athletes.csv')
coaches = spark.read.format('csv').option('header','true').load('/mnt/tokyo/raw-data/Coaches.csv')
gender = spark.read.format('csv').option('header','true').load('/mnt/tokyo/raw-data/EntriesGender.csv')
medals = spark.read.format('csv').option('header','true').option('inferSchema','true').load('/mnt/tokyo/raw-data/Medals.csv')
teams = spark.read.format('csv').option('header','true').load('/mnt/tokyo/raw-data/Teams.csv')

# COMMAND ----------

gender.printSchema()

# COMMAND ----------

gender = gender.withColumn('Female', col('Female').cast(IntegerType()))\
    .withColumn('Male', col('Male').cast(IntegerType())) \
        .withColumn('Total', col('Total').cast(IntegerType())) 
    

# COMMAND ----------

athletes.write.mode('overwrite').option('header','true').csv('/mnt/tokyo/transformed-data/athletes')
coaches.write.mode('overwrite').option('header','true').csv('/mnt/tokyo/transformed-data/coaches')
gender.write.mode('overwrite').option('header','true').csv('/mnt/tokyo/transformed-data/gender')
medals.write.mode('overwrite').option('header','true').csv('/mnt/tokyo/transformed-data/medals')
teams.write.mode('overwrite').option('header','true').csv('/mnt/tokyo/transformed-data/teams')

# COMMAND ----------


