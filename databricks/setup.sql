-- VeloRadar: Unity Catalog objects
CREATE CATALOG IF NOT EXISTS veloradar;

CREATE SCHEMA IF NOT EXISTS veloradar.landing;
CREATE VOLUME IF NOT EXISTS veloradar.landing.events;

CREATE SCHEMA IF NOT EXISTS veloradar.bronze;
CREATE SCHEMA IF NOT EXISTS veloradar.silver;
CREATE SCHEMA IF NOT EXISTS veloradar.gold;
