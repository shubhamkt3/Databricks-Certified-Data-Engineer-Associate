-- Databricks notebook source
-- MAGIC %python
-- MAGIC fire_df = spark.read \
-- MAGIC         .format('csv') \
-- MAGIC         .option('header','true') \
-- MAGIC         .option('inferSchema', "true") \
-- MAGIC         .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")   
-- MAGIC fire_df.createGlobalTempView('vw_fire_service_calls')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fire_df.describe

-- COMMAND ----------

create database if not exists demo_db

-- COMMAND ----------

create table if not exists demo_db.tbl_fire_service_calls (
  CallNumber integer, 
  UnitID string, 
  IncidentNumber integer, 
  CallType string, 
  CallDate string, 
  WatchDate string, 
  CallFinalDisposition string, 
  AvailableDtTm string, 
  Address string, 
  City string, 
  ZipcodeofIncident integer, 
  Battalion string, 
  StationArea string, 
  Box string, 
  OrigPriority string, 
  Priority string, 
  FinalPriority int, 
  ALSUnit boolean, 
  CallTypeGroup string, 
  NumAlarms int, 
  UnitType string, 
  Unitsequenceincalldispatch int, 
  FirePreventionDistrict string, 
  SupervisorDistrict string, 
  Neighborhood string, 
  Location string, 
  RowID string, 
  Delay double
) using parquet

-- COMMAND ----------

insert into demo_db.tbl_fire_service_calls
select * from global_temp.vw_fire_service_calls

-- COMMAND ----------

  truncate table demo_db.tbl_fire_service_calls

-- COMMAND ----------


