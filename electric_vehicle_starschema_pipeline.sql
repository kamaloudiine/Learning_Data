-- =========================================================
-- 1. Streaming source table
-- =========================================================
CREATE OR REFRESH STREAMING LIVE TABLE Electric_Vehicle_Population_Data
(CONSTRAINT valid_legislative_district EXPECT (legislative_district is not null) ON VIOLATION drop row)
AS
SELECT 
  `VIN (1-10)` AS VIN_1_10,
  County,
  City,
  State,
  `Postal Code` AS zipcode,
  `Model Year` AS model_year,
  Make,
  Model,
  `Electric Vehicle Type` AS electric_vehicle_type,
  `Electric Range` AS electric_range,
  `Base MSRP` AS base_msrp,
  `Legislative District` AS legislative_district,
  `DOL Vehicle ID` AS dol_vehicle_id,
  `Vehicle Location` AS vehicle_location,
  `Electric Utility` AS electric_utility,
  `2020 Census Tract` AS census_tract
FROM cloud_files('/Volumes/firstcatalog/firstschema/raw_data', 'csv');

-- =========================================================
-- 2. Dimension: Model
-- =========================================================
CREATE OR REFRESH LIVE TABLE dim_model
AS
SELECT 
  md5(concat(coalesce(Model, ''), coalesce(Make,''))) AS model_id,
  Model,
  Make
FROM workspace.default.Electric_Vehicle_Population_Data
GROUP BY Model, Make;

-- 3. Dimension: Vehicle Type
-- =========================================================
CREATE OR REFRESH LIVE TABLE dim_vehicle_type
AS
SELECT 
  md5(coalesce(electric_vehicle_type, '')) AS electric_vehicle_type_id,
  electric_vehicle_type
FROM workspace.default.Electric_Vehicle_Population_Data
GROUP BY electric_vehicle_type;

-- =========================================================

-- =========================================================
CREATE OR REFRESH LIVE TABLE dim_electric_utility
AS
SELECT
  md5(coalesce(electric_utility, '')) AS electric_utility_id,
  electric_utility
FROM workspace.default.Electric_Vehicle_Population_Data
GROUP BY electric_utility;

-- -- =========================================================
-- -- 5. Dimension: City
-- -- =========================================================
CREATE OR REFRESH LIVE TABLE dim_city
AS
SELECT
  md5(
    concat(
      coalesce(City, ''),
      coalesce(County, ''),
      coalesce(State, ''),
      coalesce(cast(zipcode as string), ''),
      coalesce(cast(legislative_district as string), '')
    )
  ) AS city_id,
  City,
  County,
  State,
  zipcode,
  legislative_district
FROM workspace.default.Electric_Vehicle_Population_Data
GROUP BY City, County, State, zipcode, legislative_district;

-- -- =========================================================
-- -- 6. Dimension: Year
-- -- =========================================================
CREATE OR REFRESH LIVE TABLE dim_year
AS
SELECT
  md5(cast(model_year as string)) AS year_id,
  model_year
FROM workspace.default.Electric_Vehicle_Population_Data
GROUP BY model_year;

-- =========================================================
-- 7. Fact Table: Vehicle
-- =========================================================
CREATE OR REFRESH LIVE TABLE fact_vehicle
AS
SELECT
  md5(
    concat(
      coalesce(cast(a.electric_range as string), ''),
      coalesce(cast(a.base_msrp as string), ''),
      coalesce(a.dol_vehicle_id, '')
    )
  ) AS vehicle_id,
  a.electric_range,
  a.base_msrp,
  a.dol_vehicle_id,
  a.model_year,
  a.electric_utility,
  a.electric_vehicle_type,
  b.city_id,
  c.year_id,
  d.electric_utility_id,
  e.electric_vehicle_type_id,
  f.model_id
FROM workspace.default.Electric_Vehicle_Population_Data a
LEFT JOIN LIVE.dim_city b
  ON md5(
    concat(
      coalesce(a.City, ''),
      coalesce(a.County, ''),
      coalesce(a.State, ''),
      coalesce(cast(a.zipcode as string), ''),
      coalesce(cast(a.legislative_district as string), '')
    )
  ) = b.city_id
LEFT JOIN workspace.default.dim_year c
  ON md5(cast(a.model_year as string)) = c.year_id
LEFT JOIN workspace.default.dim_electric_utility d
  ON md5(coalesce(a.electric_utility, '')) = d.electric_utility_id
LEFT JOIN workspace.default.dim_vehicle_type e
  ON md5(coalesce(a.electric_vehicle_type, '')) = e.electric_vehicle_type_id
LEFT JOIN workspace.default.dim_model f
  ON md5(concat(coalesce(a.Model, ''), coalesce(a.Make, ''))) = f.model_id;

