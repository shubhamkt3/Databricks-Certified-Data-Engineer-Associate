# Databricks notebook source
input_path = '/mnt/silver/Sales'
table_name = []
for path in dbutils.fs.ls(input_path):
    table_name.append(path.name[:-1])

for table in table_name:
    delta_path = f'{input_path}/{table}/'

    df = spark.read.format('delta').load(delta_path)
    columns = df.columns

    for old_col in columns:

        new_col_name = ''.join(['_'+ char if char.isupper() and not old_col[i-1].isupper() else char for i,char in enumerate(old_col)]).lstrip('_')
        df = df.withColumnRenamed(old_col, new_col_name)

    delta_output = f'/mnt/gold/Sales/{table}'
    df.write.format('delta').mode('overwrite').option("mergeSchema", "true").save(delta_output)


# COMMAND ----------



# COMMAND ----------


