# Databricks notebook source
from pyspark.sql.functions import to_date, to_timestamp, round, count_distinct, expr

# COMMAND ----------


raw_fire_df = spark.read \
        .format('csv') \
       .option('header','true') \
        .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")  

# COMMAND ----------

display(raw_fire_df)

# COMMAND ----------

renamed_fire_df = raw_fire_df \
    #.withColumnRenamed('Call Number', 'CallNumber') \
    #.withColumnRenamed('Unit ID', 'UnitID')

# COMMAND ----------

for col in raw_fire_df.columns:
    new_col = col.replace(' ','')
    renamed_fire_df = renamed_fire_df.withColumnRenamed(col, new_col)


# COMMAND ----------

renamed_fire_df.printSchema()

# COMMAND ----------

fire_df = renamed_fire_df \
    .withColumn("CallDate", to_date('CallDate','MM/dd/yyyy')) \
    .withColumn("WatchDate", to_date('WatchDate','MM/dd/yyyy')) \
    .withColumn("AvailableDtTm", to_timestamp('AvailableDtTm','MM/dd/yyyy hh:mm:ss a'))    \
    .withColumn('Delay', round('Delay',2))         

# COMMAND ----------

fire_df.printSchema()

# COMMAND ----------

fire_df.cache()

# COMMAND ----------

q1_df = fire_df.where('CallType is not null') \
    .select('CallType')  \
    .distinct()
display(q1_df.count())

# COMMAND ----------

q2_df = fire_df.where('CallType is not null') \
    .select(expr('CallType as distinct_call_types')) \
    .distinct()
q2_df.show()

# COMMAND ----------

fire_df.where('Delay > 5') \
    .select('CallNumber','Delay')  \
    .show()


# COMMAND ----------

fire_df.where('CallType is not null') \
    .select('CallType')  \
    .groupby('CallType') \
    .count()  \
    .orderBy('count', ascending=False) \
    .show()

# COMMAND ----------


fire_df.select('CallType','ZipcodeofIncident') \
        .groupBy('CallType','ZipcodeofIncident') \
        .count() \
        .orderBy('count', ascending=False) \
        .show()

# COMMAND ----------

fire_df.where('ZipcodeofIncident in (94102,94103)') \
    .select('ZipcodeofIncident','Neighborhood') \
    .show()

# COMMAND ----------

select sum(NumAlarms), avg(Delay) as delay, min(Delay) as min, Max(Delay) as max
from demo_db.tbl_fire_service_calls

# COMMAND ----------

fire_df.select(sum('NumAlarms').alias('sum'), avg('Delay'), min('Delay'), max('Delay')) \
    .show()

# COMMAND ----------

from pyspark.sql.functions import sum, avg, min, max, year, weekofyear

# COMMAND ----------

Select distinct year(to_date(callDate, "yyyy-MM-dd"))
from demo_db.tbl_fire_service_calls

# COMMAND ----------

fire_df.select(year('CallDate').alias('Year')) \
    .distinct() \
        .orderBy('Year') \
        .show()

# COMMAND ----------

Select weekofyear(to_date(callDate, "yyyy-MM-dd")), count(*)
from demo_db.tbl_fire_service_calls
where year(to_date(callDate, "yyyy-MM-dd")) = 2018
group by weekofyear(to_date(callDate, "yyyy-MM-dd"))
order by 2 desc

# COMMAND ----------

fire_df.select(weekofyear('CallDate').alias('weekOfYear')) \
    .where('year(CallDate)=2018') \
        .groupBy('weekOfYear') \
            .count() \
                .orderBy('count', ascending=False) \
            .show()

# COMMAND ----------

 Select Neighborhood, Delay
from demo_db.tbl_fire_service_calls
where City = 'San Francisco' and year(to_date(CallDate,'yyyy-MM-dd'))==2018
order by Delay desc

# COMMAND ----------

fire_df.select('Neighborhood','Delay') \
    .where((fire_df['City'] == "San Francisco") & ( year("CallDate")==2018)) \
    .orderBy('Delay', ascending=False) \
    .show()    

# COMMAND ----------

fire_df.select('Neighborhood', 'Delay') \
    .where((fire_df['City'] == 'San Francisco') & (year('CallDate') == 2018)) \
    .orderBy('Delay', ascending=False) \
    .show()

# COMMAND ----------


