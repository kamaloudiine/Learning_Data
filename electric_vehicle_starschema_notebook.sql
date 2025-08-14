-- Databricks notebook source
--import the data--
SELECT * FROM firstcatalog.firstschema.electric_vehicle_population_data

-- COMMAND ----------

--dim_model--
DROP table if exists firstcatalog.firstschema.dim_model;
CREATE table if not exists firstcatalog.firstschema.dim_model
SELECT 
md5(concat(coalesce(Model, ''), coalesce(Make,''))) AS model_id,
Model
,Make 
FROM firstcatalog.firstschema.electric_vehicle_population_data  WHERE md5(concat(coalesce(Model, ''), coalesce(Make,''))) IS NOT NULL
GROUP BY Model, Make 




-- COMMAND ----------

SELECT * FROM firstcatalog.firstschema.dim_model

-- COMMAND ----------


DROP table if exists  firstcatalog.firstschema.dim_vehicle_type;
CREATE table if not exists firstcatalog.firstschema.dim_vehicle_type
SELECT 
md5(concat(coalesce(`Electric Vehicle Type`, ''))) AS vehicle_type_id,
`Electric Vehicle Type` as electric_vehicle_type
FROM firstcatalog.firstschema.electric_vehicle_population_data WHERE md5(concat(coalesce(`Electric Vehicle Type`, ''))) IS NOT NULL
GROUP BY `Electric Vehicle Type`

-- COMMAND ----------

DROP table if exists firstcatalog.firstschema.dim_electric_utility;
CREATE table if not exists firstcatalog.firstschema.dim_electric_utility
SELECT
md5(concat(coalesce(`Electric Utility`, ''))) AS electric_utility_id,
`Electric Utility` as electric_utility
FROM firstcatalog.firstschema.electric_vehicle_population_data WHERE md5(concat(coalesce(`Electric Utility`, ''))) IS NOT NULL
GROUP BY `Electric Utility`

-- COMMAND ----------

DROP table if exists firstcatalog.firstschema.dim_city;

CREATE table if not exists firstcatalog.firstschema.dim_city
SELECT 
--dim_city--
md5(concat(coalesce(City, ''),coalesce(County, ''),coalesce(State, ''),coalesce(`Postal Code`,cast(NULL as INT)),coalesce(`Legislative District`, cast(NULL as INT)))) as city_id
,City 
,County
,State
,`Postal Code`as postal_code
,`Legislative District` as legislative_district --dim_city
--,COUNT(1) 
FROM firstcatalog.firstschema.electric_vehicle_population_data 
WHERE md5(concat(coalesce(City, ''),coalesce(County, ''),coalesce(State, ''),coalesce(`Postal Code`,cast(NULL as INT)),coalesce(`Legislative District`, cast(NULL as INT)))) IS NOT NULL
GROUP BY City,County,State,`Postal Code`,`Legislative District`
--HAVING COUNT(1) > 1

-- COMMAND ----------

SELECT
  city_id,
  count(1)
FROM
  firstcatalog.firstschema.dim_city
GROUP BY
  city_id
HAVING
  count(1) > 1

-- COMMAND ----------

--dim_year--
DROP table if exists firstcatalog.firstschema.dim_year;
CREATE table if not exists firstcatalog.firstschema.dim_year
SELECT
md5(concat(coalesce(`Model Year`,''))) as year_id
, `Model Year` as model_year
FROM firstcatalog.firstschema.electric_vehicle_population_data WHERE md5(concat(coalesce(`Model Year`,''))) IS NOT NULL
Group by `Model Year`

-- COMMAND ----------

--fact_vehicle--
DROP TABLE IF EXISTS firstcatalog.firstschema.fact_vehicle;

CREATE TABLE IF NOT EXISTS firstcatalog.firstschema.fact_vehicle AS
SELECT
  --fact_vehicle--
  md5(
    concat(
      coalesce(a.Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility, ''),
      coalesce(a.`Electric Range`, cast(null as int)),
      coalesce(a.`Base MSRP`, cast(null as int)),
      coalesce(a.`DOL Vehicle ID`, ''),
      coalesce(a.`Vehicle Location`, '')
    )
  ) as vehicle_id,
  a.Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility,
  a.`Electric Range` as electric_range,
  a.`Base MSRP` as base_msrp,
  a.`DOL Vehicle ID` as dol_vehicle_id,
  a.`Vehicle Location` as vehicle_location,
  a.`Model Year` as model_year,
  a.`Electric Utility` as electric_utility,
  a.`Electric Vehicle Type` as electric_vehicle_type,
  b.city_id,
  c.year_id,
  d.electric_utility_id,
  e.vehicle_type_id,
  f.model_id
FROM
  firstcatalog.firstschema.electric_vehicle_population_data a
  LEFT JOIN firstcatalog.firstschema.dim_city b ON md5(
    concat(
      coalesce(a.City, ''),
      coalesce(a.County, ''),
      coalesce(a.State, ''),
      coalesce(a.`Postal Code`, cast(NULL as INT)),
      coalesce(a.`Legislative District`, cast(NULL as INT))
    )
  ) = b.city_id
  LEFT JOIN firstcatalog.firstschema.dim_year c ON md5(concat(coalesce(a.`Model Year`, ''))) = c.year_id
  LEFT JOIN firstcatalog.firstschema.dim_electric_utility d ON md5(concat(coalesce(a.`Electric Utility`, ''))) = d.electric_utility_id
  LEFT JOIN firstcatalog.firstschema.dim_vehicle_type e ON md5(coalesce(a.`Electric Vehicle Type`, '')) = e.vehicle_type_id
  LEFT JOIN firstcatalog.firstschema.dim_model f ON md5(
    concat(coalesce(a.Model, ''), coalesce(a.Make, ''))
  ) = f.model_id
GROUP BY
  a.Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility,
  a.`Electric Range`,
  a.`Base MSRP`,
  a.`DOL Vehicle ID`,
  a.`Vehicle Location`,
  a.`Model Year`,
  a.`Electric Utility`,
  a.`Electric Vehicle Type`,
  b.city_id,
  c.year_id,
  d.electric_utility_id,
  e.vehicle_type_id,
  f.model_id;

-- COMMAND ----------



