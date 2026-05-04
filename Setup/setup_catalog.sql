-- Creating Catalogs
CREATE CATALOG IF NOT EXISTS pfizer;
USE CATALOG pfizer;

-- Creating Medallion Architecture schema
CREATE SCHEMA IF NOT EXISTS pfizer.bronze;
CREATE SCHEMA IF NOT EXISTS pfizer.silver;
CREATE SCHEMA IF NOT EXISTS pfizer.gold;