/*
================================================
Create Database and Schemas
================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exist.
    If the database exist, it'll be droped and then recreated.
    Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this scripts will drop the entire 'DataWarehouse' database if it exist.
    All the data in a database will be permanently deleted.
    Procede with caution and ensure you have proper backups before running this script.
*/

USE master;
GO

--Drop and recreate the 'DataWarehouse' database
IF EXIST (SELECT 1 FROM sys.database WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
  
--Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO
  
-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
