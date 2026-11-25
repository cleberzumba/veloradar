-- VeloRadar: Unity Catalog objects for each environment (dev, stage, prod)
-- Run once in the Databricks SQL Editor before the first bundle deploy.

-- dev
CREATE CATALOG IF NOT EXISTS veloradar_dev;
CREATE SCHEMA  IF NOT EXISTS veloradar_dev.landing;
CREATE VOLUME  IF NOT EXISTS veloradar_dev.landing.events;
CREATE SCHEMA  IF NOT EXISTS veloradar_dev.bronze;
CREATE SCHEMA  IF NOT EXISTS veloradar_dev.silver;
CREATE SCHEMA  IF NOT EXISTS veloradar_dev.gold;

-- stage
CREATE CATALOG IF NOT EXISTS veloradar_stage;
CREATE SCHEMA  IF NOT EXISTS veloradar_stage.landing;
CREATE VOLUME  IF NOT EXISTS veloradar_stage.landing.events;
CREATE SCHEMA  IF NOT EXISTS veloradar_stage.bronze;
CREATE SCHEMA  IF NOT EXISTS veloradar_stage.silver;
CREATE SCHEMA  IF NOT EXISTS veloradar_stage.gold;

-- prod
CREATE CATALOG IF NOT EXISTS veloradar_prod;
CREATE SCHEMA  IF NOT EXISTS veloradar_prod.landing;
CREATE VOLUME  IF NOT EXISTS veloradar_prod.landing.events;
CREATE SCHEMA  IF NOT EXISTS veloradar_prod.bronze;
CREATE SCHEMA  IF NOT EXISTS veloradar_prod.silver;
CREATE SCHEMA  IF NOT EXISTS veloradar_prod.gold;
