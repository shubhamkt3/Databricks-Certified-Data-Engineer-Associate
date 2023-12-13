# Databricks notebook source
# MAGIC %md
# MAGIC Transformation for all tables
# MAGIC

# COMMAND ----------

table_name = []
for path in dbutils.fs.ls('mnt/bronze/Sales'):
    table_name.append(path.name.split('/')[0])

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

for table in table_name:
    parquet_path = f"/mnt/bronze/Sales/{table}/{table}.parquet"
    df = spark.read.format('parquet').load(parquet_path)
    columns = df.columns

    for col in columns:
        if 'date' in col or 'Date' in col:
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))

    output_path = f'/mnt/silver/Sales/{table}/'
    df.write.format('delta').mode('overwrite').save(output_path)


# COMMAND ----------


