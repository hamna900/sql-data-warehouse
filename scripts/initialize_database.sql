/*
This script sets up the initial Data Warehouse environment.
*/

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
END
GO

USE DataWarehouse;
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Bronze') EXEC('CREATE SCHEMA Bronze');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Silver') EXEC('CREATE SCHEMA Silver');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Gold') EXEC('CREATE SCHEMA Gold');
GO

