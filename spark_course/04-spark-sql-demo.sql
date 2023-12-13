-- Databricks notebook source
select * from demo_db.tbl_fire_service_calls

-- COMMAND ----------

select count(distinct CallType) from demo_db.tbl_fire_service_calls

-- COMMAND ----------

select distinct CallType from demo_db.tbl_fire_service_calls

-- COMMAND ----------

select CallNumber, Delay
from demo_db.tbl_fire_service_calls
where Delay>5

-- COMMAND ----------

Select CallType, count(*)
from demo_db.tbl_fire_service_calls
group by CallType
order by 2 desc

-- COMMAND ----------

Select CallType, ZipcodeofIncident, count(*) as count
from demo_db.tbl_fire_service_calls
group by CallType, ZipcodeofIncident
order by count desc

-- COMMAND ----------

Select distinct ZipcodeofIncident, Neighborhood
from demo_db.tbl_fire_service_calls
where ZipcodeofIncident in (94102,94103)

-- COMMAND ----------

select sum(NumAlarms), avg(Delay) as delay, min(Delay) as min, Max(Delay) as max
from demo_db.tbl_fire_service_calls

-- COMMAND ----------

Select distinct year(to_date(callDate, "yyyy-MM-dd"))
from demo_db.tbl_fire_service_calls

-- COMMAND ----------

Select weekofyear(to_date(callDate, "yyyy-MM-dd")), count(*)
from demo_db.tbl_fire_service_calls
where year(to_date(callDate, "yyyy-MM-dd")) = 2018
group by weekofyear(to_date(callDate, "yyyy-MM-dd"))
order by 2 desc

-- COMMAND ----------

Select Neighborhood, Delay
from demo_db.tbl_fire_service_calls
where City = 'San Francisco' and year(to_date(CallDate,'yyyy-MM-dd'))==2018
order by Delay desc

-- COMMAND ----------

print('df')
